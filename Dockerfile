FROM golang:1.26.8-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api \
    && CGO_ENABLED=0 go build -trimpath -o /out/migrate ./cmd/migrate \
    && CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker

FROM alpine:3.23
RUN apk add --no-cache ca-certificates \
    && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=build /out/api /out/migrate /out/worker /app/
USER app
EXPOSE 8080
CMD ["/app/api"]
