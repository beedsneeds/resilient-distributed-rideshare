FROM golang:1.25

WORKDIR /build

COPY services/matching/go.mod go.mod
COPY services/matching/go.sum go.sum
RUN go mod download

COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.38 /ko-app/grpc-health-probe /bin/grpc_health_probe

COPY services/matching/ .

RUN go build -o /matching .

EXPOSE 50053

CMD ["/matching"]
