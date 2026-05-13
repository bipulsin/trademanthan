# syntax=docker/dockerfile:1.20
# Build context must be vendor/paperclip-subpath (contains patches/).
# Produces the same runtime layout as upstream paperclip Dockerfile, with SPA base path support.

ARG PAPERCLIP_UPSTREAM_REF=b947a7d76c331b3ce4069d3be0ade25cc89b1b90

FROM node:lts-trixie-slim AS patched-source
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates git \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
ARG PAPERCLIP_UPSTREAM_REF
RUN git init \
  && git remote add origin https://github.com/paperclipai/paperclip.git \
  && git fetch --depth 1 origin "${PAPERCLIP_UPSTREAM_REF}" \
  && git checkout FETCH_HEAD
COPY patches/0001-spa-subpath.patch /tmp/0001-spa-subpath.patch
RUN git apply /tmp/0001-spa-subpath.patch

FROM node:lts-trixie-slim AS base
ARG USER_UID=1000
ARG USER_GID=1000
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates gosu curl gh git wget ripgrep python3 \
  && rm -rf /var/lib/apt/lists/* \
  && corepack enable
RUN usermod -u $USER_UID --non-unique node \
  && groupmod -g $USER_GID --non-unique node \
  && usermod -g $USER_GID -d /paperclip node

FROM base AS deps
WORKDIR /app
COPY --from=patched-source /src/ /app/
RUN pnpm install --frozen-lockfile

FROM base AS build
WORKDIR /app
COPY --from=deps /app /app
ARG PAPERCLIP_UI_BASE_PATH=/
ENV PAPERCLIP_UI_BASE_PATH=${PAPERCLIP_UI_BASE_PATH}
RUN pnpm --filter @paperclipai/ui build
RUN pnpm --filter @paperclipai/plugin-sdk build
RUN pnpm --filter @paperclipai/server build
RUN test -f server/dist/index.js || (echo "ERROR: server build output missing" && exit 1)

FROM base AS production
ARG USER_UID=1000
ARG USER_GID=1000
WORKDIR /app
COPY --chown=node:node --from=build /app /app
RUN npm install --global --omit=dev @anthropic-ai/claude-code@latest @openai/codex@latest opencode-ai \
  && apt-get update \
  && apt-get install -y --no-install-recommends openssh-client jq \
  && rm -rf /var/lib/apt/lists/* \
  && mkdir -p /paperclip \
  && chown node:node /paperclip
COPY --from=build /app/scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENV NODE_ENV=production \
  HOME=/paperclip \
  HOST=0.0.0.0 \
  PORT=3100 \
  SERVE_UI=true \
  PAPERCLIP_HOME=/paperclip \
  PAPERCLIP_INSTANCE_ID=default \
  USER_UID=${USER_UID} \
  USER_GID=${USER_GID} \
  PAPERCLIP_CONFIG=/paperclip/instances/default/config.json \
  PAPERCLIP_DEPLOYMENT_MODE=authenticated \
  PAPERCLIP_DEPLOYMENT_EXPOSURE=private \
  OPENCODE_ALLOW_ALL_MODELS=true
VOLUME ["/paperclip"]
EXPOSE 3100
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["node", "--import", "./server/node_modules/tsx/dist/loader.mjs", "server/dist/index.js"]
