FROM golang:1.25

WORKDIR /build

COPY services/matching/go.mod go.mod
COPY services/matching/go.sum go.sum
RUN go mod download

COPY services/matching/ .

RUN go build -o /matching .

EXPOSE 50053

CMD ["/matching"]
