FROM golang:1.14.1 AS build-env
# Copy the source from the current directory to the Working Directory inside the container
COPY . .
# build binary
COPY go.mod .
COPY go.sum .

# RUN go mod download
RUN go build -o ./build/goapp ./src
RUN go build -o ./build/goemail ./EmailService
RUN go build -o ./build/gomsg ./MessageService
RUN go build -o ./build/goconsumer ./consumer
RUN go build -o ./build/goproducer ./producer

# final stage
FROM alpine
# Set the Current Working Directory inside the container
WORKDIR C:/Users/hp/Documents/GitHub/ASSIGNMENT-2
COPY --from=build-env ./build/* /app/
EXPOSE 8080
CMD ./goapp