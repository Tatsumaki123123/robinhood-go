FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/robinhood ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/robinhood /app/robinhood
COPY migrations /app/migrations
COPY docs /app/docs
EXPOSE 3000
ENTRYPOINT ["/app/robinhood"]
