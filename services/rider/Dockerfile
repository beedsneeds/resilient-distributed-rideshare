FROM mcr.microsoft.com/devcontainers/go:2-1.25-trixie

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends postgresql-client \
    && PB_REL="https://github.com/protocolbuffers/protobuf/releases" \
    && curl -LO $PB_REL/download/v33.5/protoc-33.5-linux-x86_64.zip \
    && unzip protoc-33.5-linux-x86_64.zip -d /usr/local \
    && rm protoc-33.5-linux-x86_64.zip

USER vscode

RUN go env -w GOPROXY=https://proxy.golang.org,direct \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest \
    && go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

ENV PATH="$PATH:/home/vscode/go/bin"

USER root

# Copy everything to /app
WORKDIR /app

COPY services/rider/go.mod ./
COPY services/rider/go.sum* ./
RUN go mod download

COPY services/rider/ .
COPY proto/ proto/

RUN go build -o /usr/local/bin/rider .

EXPOSE 50052

CMD ["/usr/local/bin/rider"]