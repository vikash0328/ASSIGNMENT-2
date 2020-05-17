FROM golang:1.13.3
WORKDIR C:/Users/hp/Documents/GitHub/ASSIGNMENT-2/EmailService/
RUN go get -u go.mongodb.org/mongo-driver/bson
RUN go get -u github.com/segmentio/kafka-go
RUN go get -u github.com/spf13/viper
RUN go get -u go.uber.org/zap
RUN go get -u go.uber.org/zap/zapcore
RUN go get -u go.mongodb.org/mongo-driver/bson/primitive
RUN go get -u go.mongodb.org/mongo-driver/mongo
RUN go get -u go.mongodb.org/mongo-driver/mongo/options
RUN go build -o consumer1 -i mainc.go
CMD ["C:/Users/hp/Documents/GitHub/ASSIGNMENT-2/EmailService/consumer1"]