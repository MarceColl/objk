FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release

FROM scratch AS base
EXPOSE 3000
VOLUME /data
ENTRYPOINT ["/objk", "/data"]

# CI: package prebuilt binaries from dist/, no compile per platform.
FROM base AS dist
ARG TARGETARCH
COPY dist/objk-linux-${TARGETARCH} /objk

# Default: self-contained build.
FROM base
COPY --from=build /src/target/release/objk /objk
