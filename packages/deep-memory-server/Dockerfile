FROM node:22-slim AS build

WORKDIR /repo

# pnpm via corepack (matches repo workflow)
RUN corepack enable

# Some deps may compile during install. Also, onnxruntime-node requires glibc at runtime,
# so we build + run on Debian (slim) instead of Alpine.
RUN apt-get update \
  && apt-get install -y --no-install-recommends git python3 make g++ cmake ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Workspace skeleton (keeps install cacheable)
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY tsconfig.json ./tsconfig.json

COPY packages/deep-memory-server/package.json packages/deep-memory-server/package.json

# Install only this package (do NOT use --workspace-root/-w, it forces installing root deps)
RUN pnpm install --frozen-lockfile --filter @openclaw/deep-memory-server

# Build
COPY packages/deep-memory-server/ packages/deep-memory-server/
RUN pnpm --dir packages/deep-memory-server build

# Create a production deploy directory with only prod deps + dist
# pnpm v10 requires injected workspace packages unless using legacy deploy.
RUN pnpm --filter @openclaw/deep-memory-server deploy --prod --legacy /out

FROM node:22-slim AS runtime

ENV NODE_ENV=production
WORKDIR /app

COPY --from=build /out/ /app/

EXPOSE 8088

CMD ["node", "dist/index.js"]

