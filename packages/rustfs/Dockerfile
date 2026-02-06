FROM rust:1.85-slim AS build
WORKDIR /app

# Build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
  pkg-config \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY packages/rustfs/Cargo.toml packages/rustfs/Cargo.toml
COPY packages/rustfs/src packages/rustfs/src

WORKDIR /app/packages/rustfs
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/packages/rustfs/target/release/rustfs /usr/local/bin/rustfs

ENV RUSTFS_PORT=8099
ENV RUSTFS_DATA_DIR=/data
ENV RUSTFS_DB_PATH=/data/meta.db
EXPOSE 8099

CMD ["rustfs"]

