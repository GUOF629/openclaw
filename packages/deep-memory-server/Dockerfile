FROM node:22-alpine AS build

WORKDIR /repo

# pnpm via corepack (matches repo workflow)
RUN corepack enable

# Workspace skeleton (keeps install cacheable)
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/deep-memory-server/package.json packages/deep-memory-server/package.json

# Install only what this package needs (including dev deps for build)
RUN pnpm -w install --frozen-lockfile --filter @openclaw/deep-memory-server...

# Build
COPY packages/deep-memory-server/ packages/deep-memory-server/
RUN pnpm --dir packages/deep-memory-server build

# Create a production deploy directory with only prod deps + dist
RUN pnpm --filter @openclaw/deep-memory-server deploy --prod /out

FROM node:22-alpine AS runtime

ENV NODE_ENV=production
WORKDIR /app

COPY --from=build /out/ /app/

EXPOSE 8088

CMD ["node", "dist/index.js"]

