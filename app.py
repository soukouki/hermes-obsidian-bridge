#!/usr/bin/env python3
"""
Hermes Obsidian Watcher
------------------------
Obsidianのノート内に記述された `@Hermes` を検出し、別マシン上で動作する
Hermes Agent ゲートウェイ(HTTP API)にリクエストを送り、結果をノートに
書き戻すデーモン。

検出は inotify (Linux) を使ったイベント駆動で、数千ファイル規模でも
ポーリングなしで軽量に動作する。

処理の流れ:
    1. ノート内で未処理の "@Hermes" を検出
    2. 直後を "@Hermes👀<!-- hermes-id:XXXX -->" に即座に書き換える
       (この書き換え自体もinotifyイベントを発火させるが、マーカーが
        付いているため再検出されず、二重処理を防げる)
    3. 該当行の段落 + 前後1段落分を抽出し、build_prompt() でプロンプトを
       組み立ててHermesゲートウェイに送信
    4. 結果が返ってきたら "@Hermes👀..." を "@Hermes✅️..." に置き換え、
       その段落の直後にcallout形式で結果を挿入
    5. エラー時は "@Hermes⚠️..." にし、手動で "@Hermes" に戻せば再試行可能

依存:
    pip install inotify_simple requests --break-system-packages

前提:
    - Linux上でボルトファイルシステムに直接アクセスできること(inotifyは
      Linux専用のため)。macOSの場合はfswatch等への置き換えが必要。
    - Hermesゲートウェイが起動しており、VPN越しにHTTP到達できること。
    - Hermesゲートウェイのconfig.yamlでAPIサーバーが有効化され、
      API_SERVER_KEYが設定されていること。
"""

from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from inotify_simple import INotify, flags

# ============================================================
# 設定(環境に合わせて変更してください)
# ============================================================

VAULT_DIR = Path(os.getenv("VAULT_DIR", "/vault"))
HERMES_BASE = os.getenv("HERMES_BASE", "http://localhost:8642")
API_KEY = os.getenv("API_KEY", "api_server_key")

TRIGGER_TAG = "@Hermes"
EMOJI_SEEN = "👀"     # 処理開始
EMOJI_DONE = "✅️"     # 処理完了
EMOJI_ERROR = "⚠️"    # エラー(手動でTRIGGER_TAGに書き戻せば再試行される)

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("hermes-watcher")

# ============================================================
# 正規表現
# ============================================================

# まだ絵文字が付いていない生の @Hermes を検出
TRIGGER_RE = re.compile(
    re.escape(TRIGGER_TAG)
    + r"(?!%s|%s|%s)" % tuple(re.escape(e) for e in (EMOJI_SEEN, EMOJI_DONE, EMOJI_ERROR))
)


def seen_marker_re(request_id: str) -> re.Pattern:
    """処理中マーカー(IDつき)を探すための正規表現。"""
    return re.compile(
        re.escape(f"{TRIGGER_TAG}{EMOJI_SEEN}<!-- hermes-id:{request_id} -->")
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

def split_paragraphs(text: str) -> list[tuple[int, int, str]]:
    """
    空行区切りで段落のリストを作る。各要素は (start, end, content)。
    """
    return [
        (m.start(), m.end(), m.group())
        for m in re.finditer(r"[^\n]+(?:\n(?!\s*\n)[^\n]*)*", text)
    ]


def extract_context(
    text: str, match_start: int, match_end: int
) -> tuple[list[str] | None, str, list[str] | None]:
    """
    @Hermesが含まれる段落、およびその前方5段落、後方3段落を取得する。
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

    prev_p = [p[2] for p in paragraphs[max(0, target_idx - 5):target_idx]] if target_idx > 0 else None
    next_p = [p[2] for p in paragraphs[target_idx + 1:target_idx + 4]] if target_idx + 1 < len(paragraphs) else None
    return prev_p, paragraphs[target_idx][2], next_p


# ============================================================
# プロンプト構築
# ここだけを編集すればHermesへの指示内容・トーンなどを調整できる
# ============================================================

def extract_user_request(target_paragraph: str) -> str:
    """対象段落から @Hermes 以降のユーザー要望テキストを取り出す。"""
    idx = target_paragraph.find(TRIGGER_TAG)
    if idx == -1:
        return target_paragraph.strip()
    return target_paragraph[idx + len(TRIGGER_TAG):].strip()


def build_prompt(
    note_title: str,
    prev_paragraphs: list[str] | None,
    target_paragraph: str,
    next_paragraphs: list[str] | None,
) -> str:
    """
    Hermesに渡す最終的な指示文を組み立てる。
    ノートのタイトルと前後の段落を文脈として渡し、要約の分量やトーンを
    指定している。挙動を変えたい場合はここを編集する。
    """
    parts = [
        f"あなたはObsidianのノート「{note_title}」内で呼び出されたアシスタントです。",
        "以下はそのノートの該当箇所の抜粋です。",
        "",
    ]
    if prev_paragraphs:
        parts += ["--- 前方5段落 ---", *prev_paragraphs, ""]

    parts += ["--- @Hermesが書かれた段落 ---", target_paragraph, ""]

    if next_paragraphs:
        parts += ["--- 後方3段落 ---", *next_paragraphs, ""]

    parts.append(
        "上記の文脈を踏まえて、必要であれば検索やプログラム実行などを行った上で、"
        "結果を簡潔に(目安3〜6行)日本語でまとめてください。"
        "必要に応じて、ObsidianのMCPツールを用いてこのノートを開いて確認したり、関連する他のノートを参照してください。"
        "Markdown形式で、前置きや後置きなしに本文のみを返してください。絶対に「結果はこのとおりです」「以下Markdown形式で回答します」「```markdown」などの前置き文は書かないでください。"
    )
    return "\n".join(parts)


# ============================================================
# Hermes API 呼び出し
# ============================================================

def call_hermes_api(prompt: str) -> str:
    headers = {"Authorization": f"Bearer {API_KEY}"}

    # POST /v1/runs のレスポンスは {"run_id": "...", "status": "started"}
    r = requests.post(
        f"{HERMES_BASE}/v1/runs",
        json={"input": prompt},
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    run_id = r.json()["run_id"]

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


# ============================================================
# ファイル書き換え
# ============================================================

def mark_seen(path: Path) -> tuple[str, str] | None:
    """
    ファイル内の最初の未処理 @Hermes を見つけ、👀マーカーに即座に置き換えて
    書き込む。戻り値: (request_id, プロンプト) または 未検出ならNone。
    """
    lock = get_file_lock(str(path))
    with lock:
        text = path.read_text(encoding="utf-8")
        m = TRIGGER_RE.search(text)
        if not m:
            return None

        prev_p, target_p, next_p = extract_context(text, m.start(), m.end())
        request_id = uuid.uuid4().hex[:8]
        marker = f"{TRIGGER_TAG}{EMOJI_SEEN}<!-- hermes-id:{request_id} -->"

        new_text = text[: m.start()] + marker + text[m.end():]
        path.write_text(new_text, encoding="utf-8")

        prompt = build_prompt(
            note_title=path.stem,
            prev_paragraphs=prev_p,
            target_paragraph=target_p,
            next_paragraphs=next_p,
        )
        return request_id, prompt


def insert_result(path: Path, request_id: str, result_text: str, error: bool = False) -> None:
    """
    対応するマーカーを✅️(エラー時は⚠️)に置き換え、その段落の直後に
    callout形式で結果を挿入する。
    """
    lock = get_file_lock(str(path))
    with lock:
        text = path.read_text(encoding="utf-8")
        m = seen_marker_re(request_id).search(text)
        if not m:
            log.warning("マーカーが見つかりません: id=%s path=%s", request_id, path)
            return

        final_emoji = EMOJI_ERROR if error else EMOJI_DONE
        replaced_marker = f"{TRIGGER_TAG}{final_emoji}<!-- hermes-id:{request_id} -->"

        paragraphs = split_paragraphs(text)
        target_idx = next(
            (i for i, (s, e, _) in enumerate(paragraphs) if s <= m.start() < e), None
        )

        callout_type = "error" if error else "note"
        body_lines = result_text.splitlines() or [""]
        callout = (
            "\n\n> [!" + callout_type + "]\n"
            + "\n".join(f"> {line}" for line in body_lines)
            + "\n"
        )

        if target_idx is not None:
            para_end = paragraphs[target_idx][1]
            new_text = (
                text[: m.start()] + replaced_marker + text[m.end():para_end]
                + callout
                + text[para_end:]
            )
        else:
            new_text = text[: m.start()] + replaced_marker + callout + text[m.end():]

        path.write_text(new_text, encoding="utf-8")


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
        return  # 未処理の@Hermesなし

    request_id, prompt = result
    log.info("処理開始 id=%s file=%s", request_id, path)

    def worker() -> None:
        try:
            answer = call_hermes_api(prompt)
            insert_result(path, request_id, answer, error=False)
            log.info("処理完了 id=%s file=%s", request_id, path)
        except Exception as e:
            log.exception("Hermes呼び出しに失敗しました id=%s", request_id)
            insert_result(path, request_id, f"エラーが発生しました: {e}", error=True)

    executor.submit(worker)
    # 同一ファイルに複数の未処理@Hermesがある場合に備え、続けて再スキャン
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

    # 起動時に既存ファイル内の未処理@Hermesも一度スキャンしておく
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
