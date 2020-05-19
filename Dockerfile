FROM golang:alpine AS build-env
ADD . /src
RUN cd /src && go build -o goapp

# final stage
FROM alpine
WORKDIR C:/Users/hp/Documents/GitHub/ASSIGNMENT-2
COPY --from=build-env /src/goapp /app/
CMD ./goapp