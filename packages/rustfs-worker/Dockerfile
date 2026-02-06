FROM node:22-slim AS deps
WORKDIR /app

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc ./
COPY packages/rustfs-worker/package.json packages/rustfs-worker/package.json

RUN corepack enable && pnpm install --frozen-lockfile --filter @openclaw/rustfs-worker...

FROM node:22-slim AS build
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/pnpm-lock.yaml ./pnpm-lock.yaml
COPY --from=deps /app/package.json ./package.json
COPY --from=deps /app/pnpm-workspace.yaml ./pnpm-workspace.yaml
COPY --from=deps /app/.npmrc ./.npmrc

COPY tsconfig.json ./tsconfig.json
COPY packages/rustfs-worker/tsconfig.json packages/rustfs-worker/tsconfig.json
COPY packages/rustfs-worker/src packages/rustfs-worker/src

RUN pnpm -w --filter @openclaw/rustfs-worker... run build

FROM node:22-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages/rustfs-worker/dist ./packages/rustfs-worker/dist
COPY --from=build /app/packages/rustfs-worker/package.json ./packages/rustfs-worker/package.json

CMD ["node", "packages/rustfs-worker/dist/index.js"]
