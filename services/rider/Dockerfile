FROM golang:1.25

WORKDIR /build

COPY services/rider/go.mod go.mod
COPY services/rider/go.sum go.sum
RUN go mod download

COPY services/rider/ .

RUN go build -o /rider .

EXPOSE 50052

CMD ["/rider"]
