FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /broker ./cmd/broker
RUN go build -trimpath -ldflags="-s -w" -o /brokectl ./cmd/brokectl
RUN go build -trimpath -ldflags="-s -w" -o /gateway ./cmd/gateway

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /broker /usr/local/bin/broker
COPY --from=builder /brokectl /usr/local/bin/brokectl
COPY --from=builder /gateway /usr/local/bin/gateway
COPY --from=builder /src/configs /etc/pubsub-broker/configs
WORKDIR /etc/pubsub-broker
EXPOSE 9000 9001 8080
VOLUME ["/data"]
ENV BROKER_DATA_DIR=/data
ENTRYPOINT ["/usr/local/bin/broker", "-config", "/etc/pubsub-broker/configs/broker.json"]
