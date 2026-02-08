FROM mcr.microsoft.com/devcontainers/go:2-1.25-trixie

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    # && apt-get -y install --no-install-recommends \
    && PB_REL="https://github.com/protocolbuffers/protobuf/releases" \
    && curl -LO $PB_REL/download/v33.5/protoc-33.5-linux-x86_64.zip \
    && unzip protoc-33.5-linux-x86_64.zip -d /usr/local \
    && rm protoc-33.5-linux-x86_64.zip


# [Optional] Uncomment the next lines to use go get to install anything else you need
USER vscode
# RUN go get -x <your-dependency-or-tool>

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
ENV PATH="$PATH:$(go env GOPATH)/bin"
# idk if expansion like this works
USER root