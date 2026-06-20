# Multi-stage build: compile a static node binary, ship it on distroless.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/raftnode ./server

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/raftnode /raftnode
ENTRYPOINT ["/raftnode"]
