# Build stage: compile amber-store with cgo so pebble uses the native
# DataDog/zstd compression. Both stages are musl/Alpine, so the dynamically
# linked binary runs in the runtime image.
FROM golang:1.26-alpine AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -ldflags='-s -w' \
      -o /amber-store ./cmd/amber-store

# Runtime stage: small Alpine image with a shell for debugging.
FROM alpine:3
RUN adduser -D -u 65532 amber && mkdir -p /data && chown amber /data
COPY --from=build /amber-store /usr/local/bin/amber-store
USER amber
# The store lives in /data; the server auto-generates its SSH identity there
# on first start. Set AMBER_ADMIN_PASSWORD to enable the /admin/ web UI.
VOLUME /data
EXPOSE 8590
ENTRYPOINT ["amber-store"]
CMD ["serve", "--store", "/data"]
