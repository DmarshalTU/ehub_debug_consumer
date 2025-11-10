# Build stage
# Docker buildx will handle cross-compilation automatically when --platform is specified
FROM rust:latest as builder

WORKDIR /app

# Copy dependency manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./

# Build dependencies only (this layer will be cached if dependencies don't change)
# We need a minimal source file for Cargo to compile dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy actual source code
COPY src ./src

# Build the application
RUN cargo build --release

# Runtime stage - explicitly set platform for final image
FROM debian:bookworm-slim

# Install CA certificates for TLS connections
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary from builder stage
COPY --from=builder /app/target/release/ehub_debug_consumer /app/ehub_debug_consumer

# Create non-root user for security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["/app/ehub_debug_consumer"]
