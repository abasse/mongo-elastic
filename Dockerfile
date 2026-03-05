# ── Build stage ───────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

# Install build dependencies.
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Cache module downloads separately from application code.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s" -trimpath -o /bin/mongo-elastic .

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM scratch

# Copy TLS certificates so the binary can reach HTTPS endpoints.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=builder /bin/mongo-elastic /mongo-elastic

# Volume for the file-based resume token (TOKEN_STORE=file).
VOLUME ["/data"]
ENV TOKEN_FILE=/data/resume_token.json

ENTRYPOINT ["/mongo-elastic"]
