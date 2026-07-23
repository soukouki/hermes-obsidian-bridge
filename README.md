# hermes-obsidian-bridge

## Hermes APIについて

`config.yaml` にて `provider` と `model` を指定できるようになっていますが、2026-07-23現在の最新版のHermesにおいて、この機能は動作しません。動作させるには、 [feat(api): honor provider-aware request routing by abundantbeing · Pull Request #54426 · NousResearch/hermes-agent · GitHub](https://github.com/NousResearch/hermes-agent/pull/54426) の変更を取り込む必要があります。

具体的には、hermes-gateway側のDockerfile等にて以下のコマンドを実行する必要があります。また、今後のHermesの更新によって動作しなくなる可能性もあります。
```sh
RUN curl -o \
    /opt/hermes/gateway/platforms/api_server.py \
    https://raw.githubusercontent.com/abundantbeing/hermes-agent/731e52fb67a525f92396bbf9c1e1704b5fedd9be/gateway/platforms/api_server.py
```

## `compose.yml`の設定例

gatewayと同じnetworkで動作させる必要があります。

```compose.yml
services:
  hermes-gateway:
    image: nousresearch/hermes-agent:latest
    restart: unless-stopped
    command: gateway run

  hermes-obsidian:
    build:
      context: ./hermes-obsidian
      dockerfile: Dockerfile
    restart: unless-stopped
    network_mode: "service:hermes-gateway"
    depends_on:
      - hermes-gateway
    volumes:
      - ./hermes-obsidian:/app
      - ${OBSIDIAN_VAULT}:/vault
    environment:
      - VAULT_DIR=/vault
      - HERMES_BASE=http://localhost:8642
      - API_KEY=${HERMES_API_SERVER_KEY}
```
