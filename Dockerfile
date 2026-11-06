# syntax=docker/dockerfile:1

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY redis/go.mod redis/go.sum ./redis/
RUN cd redis && GOWORK=off go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
WORKDIR /src/redis
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags "-s -w -X github.com/ok0/go-microgateway/internal/cli.version=${VERSION} -X github.com/ok0/go-microgateway/internal/cli.commit=${COMMIT} -X github.com/ok0/go-microgateway/internal/cli.date=${DATE}" -o /out/microgateway ./cmd/microgateway

# The proxy port is the listener address of the mounted config (8080 in the examples).
# The admin port is 9901; set admin.address to 0.0.0.0:9901 to reach it from outside the container.
# The configuration is mounted at /etc/microgateway/config.yaml.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/microgateway /microgateway
USER nonroot
EXPOSE 8080 9901
ENTRYPOINT ["/microgateway"]
CMD ["run", "-config", "/etc/microgateway/config.yaml"]
