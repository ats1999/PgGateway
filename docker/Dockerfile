
# Build stage
FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p pg-gateway

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/pg-gateway /usr/local/bin/pg-gateway

EXPOSE 6432

ENV PG_GATEWAY_CONFIG=/etc/pg-gateway/config.hcl

ENTRYPOINT ["pg-gateway"]
