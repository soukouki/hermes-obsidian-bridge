#!/usr/bin/env python3
"""
Hermes Obsidian Bridge
------------------------
Obsidianのノート内に記述された `@Hermes` のようなタグを検出し、別マシン上で動作する
Hermes Agent ゲートウェイ(HTTP API)にリクエストを送り、結果をノートに
書き戻すデーモン。

検出は inotify (Linux) を使ったイベント駆動で、数千ファイル規模でも
ポーリングなしで軽量に動作する。

処理の流れ:
    1. ノート内で未処理のタグを検出
    2. 直後を "@<Tag>👀<!-- hermes-id:XXXX -->" に即座に書き換え、
       その段落の直後に「実行中」callout(id付き)を挿入する
       (この書き換え自体もinotifyイベントを発火させるが、マーカーが
        付いているため再検出されず、二重処理を防げる)
    3. 該当行の段落 + 前後の段落を抽出し、build_prompt() でプロンプトを
       組み立ててHermesゲートウェイの POST /v1/runs に送信
    4. 実行中は GET /v1/runs/{run_id}/events (SSE) を裏で購読し、
       thinking(reasoning)やtool呼び出しの様子を蓄積する。
       ただし更新しすぎるとファイルが荒れるため、PROGRESS_UPDATE_INTERVAL秒
       ごとに、その時点までの経過を「実行中」calloutに追記していく
    5. 最終結果が返ってきたら "@<Tag>👀..." を "@<Tag>✅️..." に置き換え、
       「実行中」callout(thinkingの内容)をまるごと最終結果のcalloutで
       上書きする(thinkingの跡は残さない)
    6. エラー時は "@<Tag>⚠️..." にし、手動で "@<Tag>" に戻せば再試行可能

依存:
    pip install inotify_simple requests --break-system-packages

前提:
    - Linux上でボルトファイルシステムに直接アクセスできること(inotifyは
      Linux専用のため)。macOSの場合はfswatch等への置き換えが必要。
    - Hermesゲートウェイが起動しており、VPN越しにHTTP到達できること。
    - Hermesゲートウェイのconfig.yamlでAPIサーバーが有効化され、
      API_SERVER_KEYが設定されていること。
    - Hermesゲートウェイが GET /v1/runs/{run_id}/events (SSE) を
      提供していること(進捗表示に使用。取得できない場合でも最終結果の
      取得自体は GET /v1/runs/{run_id} のポーリングで独立して動作する)。
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import requests
import yaml
from inotify_simple import INotify, flags


# ============================================================
# 設定(環境に合わせて変更してください)
# ============================================================

VAULT_DIR = Path(os.getenv("VAULT_DIR", "/vault"))
HERMES_BASE = os.getenv("HERMES_BASE", "http://localhost:8642")
API_KEY = os.getenv("API_KEY", "api_server_key")

EMOJI_SEEN = "👀"     # 処理開始
EMOJI_DONE = "✅️"     # 処理完了
EMOJI_ERROR = "⚠️"    # エラー(手動でトリガータグに書き戻せば再試行される)

# 監視・スキャンから除外するディレクトリ名
EXCLUDE_DIRS = {".git", ".obsidian", ".trash", "node_modules"}

# Hermesへの同時リクエスト数上限
MAX_WORKERS = 4

# 1回のHermes呼び出しの完了確認ポーリング間隔・タイムアウト(秒)
# ※これは「1リクエストの完了待ち」のポーリングであり、
#   ボルト内のファイル変更検出自体はinotifyでイベント駆動のため
#   ポーリングは発生しない。
POLL_INTERVAL = 2
POLL_TIMEOUT = 600

# 実行中callout(thinking/tool呼び出しの経過)をノートに書き戻す間隔(秒)。
# 短くしすぎるとファイルの書き換えが頻発してうるさくなるので注意。
PROGRESS_UPDATE_INTERVAL = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("hermes-watcher")

# ============================================================
# エージェント設定
# ============================================================

@dataclass(frozen=True)
class AgentConfig:
    """config.yaml から読み込む1エージェントの設定。"""
    tag: str
    aliases: tuple[str, ...]
    model: str | None
    provider: str # default: "auto"
    front: str
    back: str

# デフォルト設定(config.yaml が存在しない場合のフォールバック)
DEFAULT_FRONT = (
    "あなたはObsidianのノート「{note_title}」内で呼び出されたアシスタントです。"
    "以下はそのノートの該当箇所の抜粋です。"
)
DEFAULT_BACK = (
    "上記の文脈を踏まえて、必要であれば検索やプログラム実行などを行った上で、"
    "結果を簡潔に(目安3〜6行)日本語でまとめてください。"
    "必要に応じて、ObsidianのMCPツールを用いてこのノートや関連する他のノートを参照(閲覧)しても構いませんが、"
    "このノート自体を編集・書き換えるツールは使わないでください。回答はノートを直接編集するのではなく、"
    "単純にテキストとして返してください。ノートへの反映はこの後スクリプト側で自動的に行います。"
    "Markdown形式で、前置きや後置きなしに本文のみを返してください。"
    "絶対に「結果はこのとおりです」「以下Markdown形式で回答します」「```markdown」などの前置き文は書かないでください。"
    "コールアウトの引用は自動で付与されるので不要です。"
)

CONFIG_PATH = Path(os.getenv("CONFIG_PATH", str(Path(__file__).parent / "config.yaml")))

def load_config() -> list[AgentConfig]:
    """
    config.yaml からエージェント設定を読み込む。

    戻り値: エージェントリスト (優先度順: 明示的 @ タグ > エイリアス)
    """
    if not CONFIG_PATH.exists():
        log.warning("config.yaml が見つかりません: %s (デフォルト設定を使用)", CONFIG_PATH)
        return _default_agents()

    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except Exception:
        log.exception("config.yaml の解析に失敗しました: %s (デフォルト設定を使用)", CONFIG_PATH)
        return _default_agents()

    agents_data = raw.get("agents") or {}
    default_section = raw.get("default") or {}
    default_front = default_section.get("front") or DEFAULT_FRONT
    default_back = default_section.get("back") or DEFAULT_BACK

    agents: list[AgentConfig] = []
    default_alias_holder: str | None = None

    # default セクションから front/back を補間
    def _interp(s: str) -> str:
        # config.yaml の default.front では {note_title} は補間しない
        return s

    for tag, cfg in agents_data.items():
        if not isinstance(cfg, dict):
            continue
        front = cfg.get("front") or _interp(default_front)
        back = cfg.get("back") or _interp(default_back)
        aliases = tuple(cfg.get("aliases") or [])
        model = cfg.get("model")
        provider = cfg.get("provider") or "auto"
        agents.append(AgentConfig(
            tag=tag,
            aliases=aliases,
            model=model,
            provider=provider,
            front=front,
            back=back,
        ))
        # @Hermes エイリアスを持つエージェントをデフォルトとする
        if "@Hermes" in aliases and default_alias_holder is None:
            default_alias_holder = tag

    # デフォルト設定
    if not agents:
        return _default_agents(), "@Hermes"

    return agents

def _default_agents() -> list[AgentConfig]:
    return [AgentConfig(
        tag="@Hermes",
        aliases=(),
        model=None,
        provider="auto",
        front=DEFAULT_FRONT,
        back=DEFAULT_BACK,
    )]

# 設定を1回だけロード (モジュールレベル)
_AGENTS = load_config()

# すべてのトリガータグ + エイリアス (検出/サニタイズ対象)
_ALL_TRIGGERS = set()
_TAG_TO_AGENT: dict[str, AgentConfig] = {}
for _a in _AGENTS:
    _ALL_TRIGGERS.add(_a.tag)
    _TAG_TO_AGENT[_a.tag] = _a
    for _alias in _a.aliases:
        _ALL_TRIGGERS.add(_alias)
        _TAG_TO_AGENT[_alias] = _a

# @<Tag>　のような生タグを検出 (すべての登録済みユーザー対象)
# ただしすでにマーカー付き (👀/✅️/⚠️) のものは除外
_MARKER_PREFIX = f"<!-- hermes"
_TRIBUTE_PATTERN = r"(?<!@)(" + "|".join(re.escape(t) for t in _ALL_TRIGGERS) + r")"
TRIGGER_RE = re.compile(
    _TRIBUTE_PATTERN + r"(?![👀✅⚠️])"
)


def seen_marker_re(tag: str, request_id: str) -> re.Pattern:
    """処理中マーカー(IDつき)を探すための正規表現。"""
    return re.compile(
        re.escape(f"{tag}{EMOJI_SEEN}<!-- hermes-id:{request_id} -->")
    )


# 実行中calloutを識別するためのIDコメント。
# calloutの本文側にHTMLコメントとしてIDを埋め込む(プレビュー表示では見えない)。
def progress_id_comment(request_id: str) -> str:
    return f"<!-- hermes-progress-id:{request_id} -->"


def progress_callout_re(request_id: str) -> re.Pattern:
    """実行中callout全体(タイトル行〜本文の連続する`>`行)を探す正規表現。"""
    comment = re.escape(progress_id_comment(request_id))
    return re.compile(
        r"^> \[![a-zA-Z-]+\][+-]?[^\n]*\n" # callout タイトル行
        r"> " + comment + r"\n"            # ID コメント行
        r"(?:^>.*\n)*",                    # 続く本文行(すべて`>`始まり、改行込み)
        re.MULTILINE,
    )


# ============================================================
# ファイルロック(同一ファイルへの同時書き込みを防止)
# ============================================================

_file_locks: dict[str, threading.Lock] = {}
_file_locks_guard = threading.Lock()


def get_file_lock(path: str) -> threading.Lock:
    with _file_locks_guard:
        if path not in _file_locks:
            _file_locks[path] = threading.Lock()
        return _file_locks[path]


# ============================================================
# 段落抽出(空行区切り)
# ============================================================

HEADING_RE = re.compile(r"^#{1,6}\s")


def split_paragraphs(text: str) -> list[tuple[int, int, str]]:
    """
    空行区切りで段落のリストを作る。各要素は (start, end, content)。

    加えて、見出し行(`# ...` 形式のATX見出し)は、前後に空行がなく本文と
    地続きになっていても、見出し単独で1段落として切り出す。これにより
    「見出しの直後に本文が続く」ようなケースでも、見出しと本文の間に
    自然な段落の隙間ができ、@<Tag>の直後に何かを挿入する際にその隙間を
    使えるようになる。
    """
    paragraphs: list[tuple[int, int, str]] = []
    for m in re.finditer(r"[^\n]+(?:\n(?!\s*\n)[^\n]*)*", text):
        block_start = m.start()
        lines = m.group().split("\n")

        pos = block_start
        cur_lines: list[str] = []
        cur_start = pos

        def flush() -> None:
            nonlocal cur_lines
            if cur_lines:
                content = "\n".join(cur_lines)
                paragraphs.append((cur_start, cur_start + len(content), content))
                cur_lines = []

        for line in lines:
            if HEADING_RE.match(line):
                flush()
                paragraphs.append((pos, pos + len(line), line))
                cur_start = pos + len(line) + 1
            else:
                if not cur_lines:
                    cur_start = pos
                cur_lines.append(line)
            pos += len(line) + 1
        flush()
    return paragraphs


def extract_context(
    text: str, match_start: int, match_end: int
) -> tuple[list[str] | None, str, list[str] | None]:
    """
    @<Tag>が含まれる段落、およびその前方数段落、後方数段落を取得する。
    前方は最小3段落で、多くて3000文字に収まる段落数。
    後方は最小1段落で、多くて1000文字に収まる段落数。
    戻り値: (前段落 or None, 対象段落, 次段落 or None)
    """
    paragraphs = split_paragraphs(text)
    target_idx = None
    for i, (s, e, _) in enumerate(paragraphs):
        if s <= match_start < e:
            target_idx = i
            break

    if target_idx is None:
        # 段落として拾えなかった場合は行単位にフォールバック
        line_start = text.rfind("\n", 0, match_start) + 1
        line_end = text.find("\n", match_end)
        if line_end == -1:
            line_end = len(text)
        return None, text[line_start:line_end], None
    prev_p = []
    next_p = []
    # 前方段落は最小3段落、最大3000文字まで
    for i in range(target_idx - 1, -1, -1):
        prev_p.insert(0, paragraphs[i][2])
        if len(prev_p) >= 3 and sum(len(p) for p in prev_p) >= 3000:
            break
    # もしも前方段落でファイル先頭までたどり着いていれば、`<begin of file>`を追加しておく
    if target_idx - len(prev_p) == 0:
        prev_p.insert(0, "<begin of file>")
    # 後方段落は最小1段落、最大1000文字まで
    for i in range(target_idx + 1, len(paragraphs)):
        next_p.append(paragraphs[i][2])
        if len(next_p) >= 1 and sum(len(p) for p in next_p) >= 1000:
            break
    # もしも後方段落でファイル末尾までたどり着いていれば、`<end of file>`を追加しておく
    if target_idx + len(next_p) == len(paragraphs) - 1:
        next_p.append("<end of file>")
    return prev_p, paragraphs[target_idx][2], next_p


# ============================================================
# プロンプト構築
# config.yaml の front/back でカスタマイズできる
# ============================================================

def extract_user_request(target_paragraph: str) -> str:
    """対象段落から @<Tag> 以降のユーザー要望テキストを取り出す。"""
    # Match any registered trigger tag
    for tag in _ALL_TRIGGERS:
        idx = target_paragraph.find(tag)
        if idx != -1:
            return target_paragraph[idx + len(tag):].strip()
    return target_paragraph.strip()


def build_prompt(
    agent: AgentConfig,
    note_title: str,
    prev_paragraphs: list[str] | None,
    target_paragraph: str,
    next_paragraphs: list[str] | None,
) -> str:
    """
    Hermesに渡す最終的な指示文を組み立てる。
    config.yaml の front/back を補間し、前後の段落を文脈として渡す。
    """
    parts = [agent.front.replace("{note_title}", note_title), ""]
    if prev_paragraphs:
        parts += ["--- 前方の段落 ---", *prev_paragraphs, ""]
    parts += [f"--- {agent.tag} が書かれた段落 ---", target_paragraph, ""]
    if next_paragraphs:
        parts += ["--- 後方の段落 ---", *next_paragraphs, ""]
    parts.append(agent.back)
    return "\n".join(parts)


# ============================================================
# Markdownエスケープ
# ============================================================

# calloutの本文中に差し込む動的な文字列(ツール名・引数プレビュー・
# thinkingの抜粋など)に含まれるMarkdown特殊文字をエスケープする。
# 例えば引数に `*` や `#` が入っていると、callout内で強調や見出しとして
# 解釈されて見た目が崩れるため。
_MD_ESCAPE_RE = re.compile(r"([\\`*_{}\[\]()#+\-.!>~|])")


def escape_markdown(text: str) -> str:
    if not text:
        return text
    return _MD_ESCAPE_RE.sub(r"\\\1", text)


# ============================================================
# トリガー文字列のサニタイズ
# ============================================================
#
# LLMの出力(思考経過の抜粋や最終結果)に「@Hermes」のような生の文字列が
# 含まれてしまうと、それがノート内で `TRIGGER_RE` にヒットし、
# 新しい未処理タスクとして再検知されてしまい、無限ループが発生する。
# サニタイズにより、LLMが吐いたタグに衝突するものには即座に `✅️` マークを付けて
# から書き込み、以降のスキャンで検出されないようにする。
# `✅️` は完了マーカーなので、仮に再検知されても意味論的にはヒッツに
# ならず安全に無視される(`TRIGGER_RE` はヒットしない)。
# `TRIGGER_RE` と同じ否定先頭枠守を使って生の `@<Tag>` だけを抽出する。
_SANITIZE_TRIGGER_RE = re.compile(
    _TRIBUTE_PATTERN + r"(?![👀✅⚠️])"
)

def sanitize_trigger(text: str) -> str:
    """LLM出力に含まれる生の @<Tag> を @<Tag>✅️ に置換して中立化する。"""
    if not text:
        return text
    return _SANITIZE_TRIGGER_RE.sub(lambda m: m.group(0) + EMOJI_DONE, text)


# ============================================================
# 実行中の様子(thinking / tool呼び出し)の蓄積
# ============================================================

class RunActivity:
    """
    GET /v1/runs/{run_id}/events (SSE) から届くイベントを蓄積し、
    「実行中」calloutに表示する行のリストを組み立てるためのクラス。
    複数スレッド(SSE受信スレッドとcallout更新スレッド)から
    アクセスされるためロックで保護する。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._lines: list[str] = []
        self._current_tool: str | None = None
        self._dirty = False

    def _add(self, line: str) -> None:
        with self._lock:
            self._lines.append(line)
            self._dirty = True

    @staticmethod
    def _extract_tool_name(data: dict, fallback: str | None) -> str:
        """
        ツール名のフィールド名はHermesのバージョン/エンドポイントによって
        揺れる(tool_name, name, toolName, tool.name, ...)ため、
        考えられる候補を順に見ていく。どれも見つからない場合のみ
        "tool"(不明)にフォールバックする。
        """
        candidates = (
            data.get("tool_name")
            or data.get("toolName")
            or data.get("name")
            or data.get("tool")
            or data.get("function_name")
            or data.get("functionName")
        )
        if isinstance(candidates, dict):
            candidates = (
                candidates.get("name")
                or candidates.get("tool_name")
                or candidates.get("toolName")
            )
        if isinstance(candidates, str) and candidates.strip():
            return candidates.strip()
        return fallback or "tool"

    def handle_event(self, event_type: str | None, data: dict) -> None:
        """
        SSEイベント1件を処理する。Hermesのバージョンによってフィールド名が
        揺れる可能性があるため、複数のキー名をフォールバックで見る。
        ツール呼び出しの引数・プレビューは(長い/記号が多いと読みづらいため)
        表示せず、ツール名のみを表示する。
        """
        event_type = event_type or data.get("type") or data.get("event")

        if event_type in ("reasoning.available", "reasoning.delta", "reasoning"):
            preview = (
                data.get("preview") or data.get("delta")
                or data.get("text") or data.get("message") or ""
            ).strip()
            if preview:
                # thinkingのプレビューは複数行になることがある。callout内では
                # 全ての行が`>`始まりでなければ表示が崩れるため、1行ずつ
                # 独立したエントリとして追加する(build_progress_callout側で
                # 各エントリに`> `を付けるため、ここでは改行を残さない)。
                escaped = escape_markdown(preview)
                escaped = sanitize_trigger(escaped)
                sub_lines = [ln for ln in escaped.splitlines() if ln.strip()]
                for i, ln in enumerate(sub_lines):
                    prefix = "🧠" if i == 0 else "  "
                    self._add(f"{prefix} {ln}")

        elif event_type in ("tool.started", "tool_call.started"):
            tool = self._extract_tool_name(data, None)
            self._current_tool = tool
            self._add(f"🔧 {sanitize_trigger(escape_markdown(tool))} を実行中...")

        elif event_type in ("tool.completed", "tool_call.completed"):
            tool = self._extract_tool_name(data, self._current_tool)
            self._add(f"✅ {sanitize_trigger(escape_markdown(tool))} 完了")

        elif event_type in ("tool.failed", "tool_call.failed"):
            tool = self._extract_tool_name(data, self._current_tool)
            self._add(f"⚠️ {sanitize_trigger(escape_markdown(tool))} 失敗")

        elif event_type in ("run.started",):
            self._add("🚀 実行を開始しました")

        # それ以外のイベント種別(run.completed等)は最終結果側で扱うため無視する

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self._lines)

    def pop_dirty(self) -> bool:
        """前回のcallout更新以降に新しい行が追加されたかどうかを返し、フラグを下ろす。"""
        with self._lock:
            was_dirty = self._dirty
            self._dirty = False
            return was_dirty


def stream_run_progress(run_id: str, activity: RunActivity, stop_event: threading.Event) -> None:
    """
    GET /v1/runs/{run_id}/events をSSEで購読し、届いたイベントを
    activityに書き込み続ける。接続が切れる/取得できない場合は静かに諦める
    (進捗表示が更新されないだけで、最終結果の取得は別途ポーリングで行う)。
    """
    headers = {"Authorization": f"Bearer {API_KEY}", "Accept": "text/event-stream"}
    url = f"{HERMES_BASE}/v1/runs/{run_id}/events"
    try:
        with requests.get(url, headers=headers, stream=True, timeout=(10, POLL_TIMEOUT)) as r:
            r.raise_for_status()
            event_type: str | None = None
            for raw_line in r.iter_lines(decode_unicode=True):
                if stop_event.is_set():
                    return
                if raw_line is None:
                    continue
                line = raw_line.rstrip("\r")
                if line == "":
                    event_type = None
                    continue
                if line.startswith(":"):
                    continue  # keep-alive / コメント行
                if line.startswith("event:"):
                    event_type = line[len("event:"):].strip()
                    continue
                if line.startswith("data:"):
                    data_str = line[len("data:"):].strip()
                    try:
                        data = json.loads(data_str)
                    except ValueError:
                        continue
                    activity.handle_event(event_type, data)
    except Exception:
        log.debug("SSE購読を終了しました run_id=%s", run_id, exc_info=True)


def run_progress_updater(
    path: Path, request_id: str, activity: RunActivity, done_event: threading.Event
) -> None:
    """
    PROGRESS_UPDATE_INTERVAL秒ごとに、activityに新しい行が溜まっていれば
    「実行中」calloutに反映する。done_eventがセットされたら終了する
    (完了時の最終上書きはworker側のinsert_resultが行うため、ここでは
    何もしない)。
    """
    while True:
        if done_event.wait(PROGRESS_UPDATE_INTERVAL):
            return
        if activity.pop_dirty():
            try:
                update_progress_callout(path, request_id, activity.snapshot())
            except Exception:
                log.exception("進捗calloutの更新に失敗しました id=%s", request_id)


# ============================================================
# Hermes API 呼び出し
# ============================================================

def call_hermes_api(prompt: str, model: str | None, provider: str, activity: RunActivity) -> str:
    headers = {"Authorization": f"Bearer {API_KEY}"}

    # POST /v1/runs のレスポンスは {"run_id": "...", "status": "started"}
    # model フィールドは PR #54426 パッチ適用後に有効
    body: dict = {"input": prompt, "provider": provider}
    if model:
        body["model"] = model
    r = requests.post(
        f"{HERMES_BASE}/v1/runs",
        json=body,
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    run_id = r.json()["run_id"]

    # 実行中の様子(thinking/tool呼び出し)をバックグラウンドで拾い続ける
    stop_stream = threading.Event()
    stream_thread = threading.Thread(
        target=stream_run_progress,
        args=(run_id, activity, stop_stream),
        daemon=True,
    )
    stream_thread.start()

    try:
        deadline = time.time() + POLL_TIMEOUT
        while time.time() < deadline:
            sr = requests.get(
                f"{HERMES_BASE}/v1/runs/{run_id}",
                headers=headers,
                timeout=30,
            )
            sr.raise_for_status()
            data = sr.json()
            status = data.get("status")

            if status == "completed":
                return (data.get("output") or "").strip()
            if status in ("failed", "cancelled"):
                raise RuntimeError(data.get("error") or f"run {status}")

            time.sleep(POLL_INTERVAL)

        raise TimeoutError("Hermesからの応答がタイムアウトしました")
    finally:
        stop_stream.set()
        stream_thread.join(timeout=5)


# calloutのタイトル行(`[!note]`, `[!warning]-` など)を検出する正規表現。
# `>` を剥がした後の文字列に対してマッチさせる。
_CALLOUT_HEADER_RE = re.compile(r"^\[![a-zA-Z][a-zA-Z0-9_-]*\][+-]?.*$")


def strip_wrapping_callout(text: str) -> str:
    """
    LLMの出力が、直前のcalloutの見た目に引っ張られるなどして丸ごと
    callout形式(`> [!note]\\n> ...`)で返ってきてしまうことがある。
    そのまま既存のcalloutに詰めるとcalloutの中にcalloutがネストして
    表示が崩れるため、出力の先頭がcallout形式であれば、その引用を
    1段階だけ剥がしてプレーンなテキストに戻す。calloutでなければ
    そのまま返す。
    """
    if not text:
        return text
    lines = text.split("\n")

    start = 0
    while start < len(lines) and lines[start].strip() == "":
        start += 1
    if start >= len(lines):
        return text

    first = lines[start].strip()
    if not first.startswith(">"):
        return text
    header_body = first[1:].strip()
    if not _CALLOUT_HEADER_RE.match(header_body):
        return text  # 単なる引用(`>`)であってcalloutではない場合は触らない

    rest = lines[start + 1:]
    non_empty = [l for l in rest if l.strip() != ""]
    quoted = [l for l in non_empty if l.lstrip().startswith(">")]
    if non_empty and len(quoted) < len(non_empty) * 0.8:
        return text  # 大半の行が`>`で始まっていない場合はcalloutと見なさない

    dequoted: list[str] = []
    for line in rest:
        s = line.lstrip()
        if s.startswith("> "):
            dequoted.append(s[2:])
        elif s.startswith(">"):
            dequoted.append(s[1:])
        else:
            dequoted.append(line)

    result = "\n".join(dequoted).strip("\n")
    return result if result else text


# ============================================================
# ファイル書き換え
# ============================================================

def build_progress_callout(request_id: str, lines: list[str]) -> str:
    """
    「実行中」calloutのMarkdown断片(タイトル行を含む)を組み立てる。
    タイトル行には既に [!example] のマークが付いているため絵文字は追加せず、
    IDはHTMLコメントとして本文1行目に埋め込む(プレビューでは非表示)。
    """
    header = "> [!example] Hermes 実行中...\n"
    id_line = f"> {progress_id_comment(request_id)}\n"
    body = "\n".join(f"> {line}" for line in (lines or ["⏳ 準備中..."]))
    return header + id_line + body + "\n"


def update_progress_callout(path: Path, request_id: str, lines: list[str]) -> None:
    """既存の「実行中」calloutの本文を、現時点までの経過で置き換える(追記)。"""
    lock = get_file_lock(str(path))
    with lock:
        text = path.read_text(encoding="utf-8")
        m = progress_callout_re(request_id).search(text)
        if not m:
            log.warning("進捗calloutが見つかりません: id=%s path=%s", request_id, path)
            return
        new_block = build_progress_callout(request_id, lines)
        new_text = text[: m.start()] + new_block + text[m.end():]
        path.write_text(new_text, encoding="utf-8")


def mark_seen(path: Path) -> tuple[str, str, str, str] | None:
    """
    ファイル内の最初の未処理 @<Tag> を見つけ、👀マーカーに即座に置き換えて
    書き込む。同時に、その段落の直後に「実行中」calloutを挿入する。
    戻り値: (request_id, prompt, model, tag) または 未検出ならNone。
    """
    lock = get_file_lock(str(path))
    with lock:
        text = path.read_text(encoding="utf-8")
        m = TRIGGER_RE.search(text)
        if not m:
            return None

        # マッチしたタグから対応する AgentConfig を解決
        matched_tag = m.group(1)  # re キャプチャで取得
        agent = _TAG_TO_AGENT.get(matched_tag)
        if agent is None:
            # フォールバック: デフォルトエージェント
            agent = _AGENTS[0]
            matched_tag = agent.tag
        log.info("未処理トリガーを検出: tag=%s file=%s", matched_tag, path)

        prev_p, target_p, next_p = extract_context(text, m.start(), m.end())
        request_id = uuid.uuid4().hex[:8]
        marker = f"{agent.tag}{EMOJI_SEEN}<!-- hermes-id:{request_id} -->"

        paragraphs = split_paragraphs(text)
        target_idx = next(
            (i for i, (s, e, _) in enumerate(paragraphs) if s <= m.start() < e), None
        )
        callout = build_progress_callout(request_id, ["⏳ 準備中..."])

        if target_idx is not None:
            para_end = paragraphs[target_idx][1]
            new_text = (
                text[: m.start()] + marker + text[m.end():para_end]
                + "\n\n" + callout
                + text[para_end:]
            )
        else:
            new_text = text[: m.start()] + marker + "\n\n" + callout + text[m.end():]
        log.info("👀マーカーを挿入し、進捗calloutを追加しました: id=%s file=%s", request_id, path)

        path.write_text(new_text, encoding="utf-8")

        prompt = build_prompt(
            agent=agent,
            note_title=path.stem,
            prev_paragraphs=prev_p,
            target_paragraph=target_p,
            next_paragraphs=next_p,
        )
        return request_id, prompt, agent.model, agent.provider, agent.tag


def insert_result(path: Path, request_id: str, tag: str, result_text: str, error: bool = False) -> None:
    """
    対応するトリガーマーカーを✅️(エラー時は⚠️)に置き換え、
    「実行中」calloutをまるごと最終結果のcalloutで上書きする
    (thinking等の途中経過は残さない)。
    """
    lock = get_file_lock(str(path))
    with lock:
        text = path.read_text(encoding="utf-8")

        # 1. トリガーマーカーを完了/エラーの絵文字に置き換え
        sm = seen_marker_re(tag, request_id).search(text)
        final_emoji = EMOJI_ERROR if error else EMOJI_DONE
        if sm:
            replaced_marker = f"{tag}{final_emoji}<!-- hermes-id:{request_id} -->"
            text = text[: sm.start()] + replaced_marker + text[sm.end():]
        else:
            log.warning("トリガーマーカーが見つかりません: id=%s path=%s", request_id, path)

        # 2. 「実行中」calloutを最終結果のcalloutで上書き
        callout_type = "error" if error else "note"
        if not error:
            result_text = strip_wrapping_callout(result_text)
        result_text = sanitize_trigger(result_text)
        body_lines = result_text.splitlines() or [""]
        final_callout = (
            "> [!" + callout_type + "]\n"
            + "\n".join(f"> {line}" for line in body_lines)
            + "\n"
        )

        pm = progress_callout_re(request_id).search(text)
        if pm:
            text = text[: pm.start()] + final_callout + text[pm.end():]
        else:
            # 進捗calloutが見つからない場合は、マーカー直後の段落末尾に新規追加する
            paragraphs = split_paragraphs(text)
            anchor = sm.start() if sm else 0
            target_idx = next(
                (i for i, (s, e, _) in enumerate(paragraphs) if s <= anchor < e), None
            )
            if target_idx is not None:
                para_end = paragraphs[target_idx][1]
                text = text[:para_end] + "\n\n" + final_callout + text[para_end:]
            else:
                text = text + "\n\n" + final_callout

        path.write_text(text, encoding="utf-8")


# ============================================================
# ワーカー
# ============================================================

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


def process_file(path: Path) -> None:
    try:
        result = mark_seen(path)
    except Exception:
        log.exception("mark_seenに失敗しました: %s", path)
        return
    if result is None:
        return  # 未処理のトリガーなし

    request_id, prompt, model, provider, tag = result
    log.info("処理開始 id=%s file=%s tag=%s", request_id, path, tag)

    def worker() -> None:
        activity = RunActivity()
        done_event = threading.Event()
        updater_thread = threading.Thread(
            target=run_progress_updater,
            args=(path, request_id, activity, done_event),
            daemon=True,
        )
        updater_thread.start()
        try:
            answer = call_hermes_api(prompt, model, provider, activity)
            insert_result(path, request_id, tag, answer, error=False)
            log.info("処理完了 id=%s file=%s", request_id, path)
        except Exception as e:
            log.exception("Hermes呼び出しに失敗しました id=%s", request_id)
            insert_result(path, request_id, tag, f"エラーが発生しました: {e}", error=True)
        finally:
            done_event.set()
            updater_thread.join(timeout=5)

    executor.submit(worker)
    # 同一ファイルに複数の未処理@<Tag>がある場合に備え、続けて再スキャン
    executor.submit(process_file, path)


# ============================================================
# inotify 監視
# ============================================================

WATCH_FLAGS = flags.CLOSE_WRITE | flags.CREATE | flags.MOVED_TO | flags.DELETE_SELF


def add_watches_recursive(inotify: INotify, root: Path, wd_to_path: dict[int, str]) -> None:
    for dirpath, dirnames, _ in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        wd = inotify.add_watch(dirpath, WATCH_FLAGS)
        wd_to_path[wd] = dirpath


def main() -> None:
    if not VAULT_DIR.exists():
        log.error("VAULT_DIRが存在しません: %s", VAULT_DIR)
        sys.exit(1)

    inotify = INotify()
    wd_to_path: dict[int, str] = {}
    add_watches_recursive(inotify, VAULT_DIR, wd_to_path)
    log.info("監視開始: %s (%d ディレクトリ)", VAULT_DIR, len(wd_to_path))

    # 起動時に既存ファイル内の未処理@<Tag>も一度スキャンしておく
    for md_path in VAULT_DIR.rglob("*.md"):
        if any(part in EXCLUDE_DIRS for part in md_path.parts):
            continue
        text = md_path.read_text(encoding="utf-8", errors="ignore")
        if TRIGGER_RE.search(text):
            process_file(md_path)

    while True:
        for event in inotify.read():
            dir_path = wd_to_path.get(event.wd)
            if dir_path is None:
                continue

            full_path = Path(dir_path) / event.name

            if event.mask & flags.ISDIR:
                if event.mask & flags.CREATE and event.name not in EXCLUDE_DIRS:
                    add_watches_recursive(inotify, full_path, wd_to_path)
                continue

            if not event.name.endswith(".md"):
                continue

            if event.mask & (flags.CLOSE_WRITE | flags.MOVED_TO):
                executor.submit(process_file, full_path)


if __name__ == "__main__":
    main()
