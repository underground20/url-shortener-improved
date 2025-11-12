FROM golang:1.24.7-alpine3.21 AS deps

RUN apk --no-cache add git

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

FROM deps AS builder

RUN apk --no-cache add \
    gcc \
    g++ \
    sqlite-dev \
    musl-dev

COPY . .

RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o ./server ./cmd/url-shortener

FROM alpine:3.22

RUN apk --no-cache add \
    sqlite

WORKDIR /app

COPY --from=builder /app/server /app/server
COPY --from=builder /app/config /app/config

RUN mkdir -p /app/storage

CMD ["/app/server"]