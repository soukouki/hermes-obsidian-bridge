# hermes-obsidian-bridge

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
