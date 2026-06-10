FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /main

FROM alpine:3.23.4 AS runner

RUN addgroup -S appgroup
RUN adduser -S -G appgroup -H -s /sbin/nologin appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appgroup /main /main

USER appuser

EXPOSE 8004

CMD ["/main"]
