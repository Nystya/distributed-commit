FROM golang:1.16.4

WORKDIR $GOPATH/src/github.com/Nystya/distributed-commit

COPY . .

RUN go build

EXPOSE 6000

ENTRYPOINT ["./distributed-commit"]