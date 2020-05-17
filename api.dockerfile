FROM golang:1.13.3
WORKDIR C:/Users/hp/Documents/GitHub/ASSIGNMENT-2/src
RUN go get -u github.com/gin-gonic/gin
RUN go get -u github.com/segmentio/kafka-go
RUN go get -u github.com/spf13/viper
RUN go get -u go.uber.org/zap
RUN go get -u go.uber.org/zap/zapcore
RUN go build -o producer -i mainp.go
CMD ["C:/Users/hp/Documents/GitHub/ASSIGNMENT-2/src/producer"]