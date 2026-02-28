FROM golang:1.25

WORKDIR /build

COPY services/ride/go.mod go.mod
COPY services/ride/go.sum go.sum
RUN go mod download

COPY services/ride/ .

RUN go build -o /ride .

EXPOSE 50051

CMD ["/ride"]
