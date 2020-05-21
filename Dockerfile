FROM golang:alpine AS build-env
ADD . /src
RUN cd /src && go build -o goapp

# final stage
FROM alpine
WORKDIR C:/Users/hp/Documents/GitHub/ASSIGNMENT-2
COPY --from=build-env /src/goapp /app/
CMD ./goappFROM golang:1.14.1 AS build-env

# Copy the source from the current directory to the Working Directory inside the container
COPY . .
# build binary
RUN go get github.com/gin-gonic/gin github.com/go-playground/assert/v2 github.com/natefinch/lumberjack gopkg.in/natefinch/lumberjack.v2 github.com/segmentio/kafka-go github.com/spf13/viper go.uber.org/zap go.mongodb.org/mongo-driver/mongo go.mongodb.org/mongo-driver/bson
RUN go build -o ./build/goapp ./src
RUN go build -o ./build/goemail ./EmailService
RUN go build -o ./build/gomsg ./MessageService
RUN go build -o ./build/goconsumer ./consumer
RUN go build -o ./build/goproducer ./producer

# final stage
FROM alpine
# Set the Current Working Directory inside the container
WORKDIR /app
COPY --from=build-env ./build/* /app/
EXPOSE 8080
CMD ./goapp