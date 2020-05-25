FROM golang:1.14.1 AS build-env
# FROM golang:alpine AS builder

# Set the Current Working Directory inside the container
WORKDIR C:/Users/hp/Documents/GitHub/ASSIGNMENT-2
# build binary
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the code into the container
COPY . .

# Build the application
RUN go build -o ./build/goapp ./src
RUN go build -o ./build/goemail ./EmailService
RUN go build -o ./build/gomsg ./MessageService

# Build a small image
# FROM scratch

COPY --from=build-env ./build/* /app/
EXPOSE 8080
CMD ./goapp







