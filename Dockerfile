FROM golang:1.21 as base

WORKDIR /opt/validate-article-json

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY main.go main_test.go manage.sh ./
RUN ./manage.sh test
