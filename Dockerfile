# The API server (`backend`) and the admin CLI (`admin`) in one small image.
# Fly builds it remotely: `fly deploy` (see fly.toml).

FROM rust:1.99-slim-trixie AS build
WORKDIR /app

# Dependencies first, against empty sources, so a code change doesn't
# rebuild them.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin \
 && echo 'fn main() {}' > src/main.rs \
 && echo 'fn main() {}' > src/bin/admin.rs \
 && touch src/lib.rs \
 && cargo build --release --locked --bins \
 && rm -rf src

# The real sources. `migrate!` embeds migrations/ into the binary. Touching
# the crate roots makes Cargo rebuild them even when the copied files are
# older than the placeholder build.
COPY src src
COPY migrations migrations
RUN touch src/main.rs src/lib.rs src/bin/admin.rs \
 && cargo build --release --locked --bins

FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --no-create-home tokenledger
COPY --from=build /app/target/release/backend /app/target/release/admin /usr/local/bin/
USER tokenledger
ENV BIND_ADDR=0.0.0.0:8080 \
    RUST_LOG=info
EXPOSE 8080
CMD ["backend"]
