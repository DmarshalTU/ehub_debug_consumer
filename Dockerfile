# Build stage
# Use BUILDPLATFORM to allow building on any architecture (Mac, Linux, etc.)
FROM --platform=$BUILDPLATFORM rust:1.75 as builder

# Install cross-compilation toolchain for Linux/amd64 target
RUN rustup target add x86_64-unknown-linux-gnu && \
    apt-get update && \
    apt-get install -y gcc-x86-64-linux-gnu && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./

# Build dependencies only (this layer will be cached if dependencies don't change)
# We need a minimal source file for Cargo to compile dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-gnu && \
    rm -rf src

# Copy actual source code
COPY src ./src

# Build the application for Linux/amd64 target
RUN cargo build --release --target x86_64-unknown-linux-gnu

# Runtime stage - use the target platform explicitly
FROM --platform=linux/amd64 debian:bookworm-slim

# Install CA certificates for TLS connections
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary from builder stage
COPY --from=builder /app/target/x86_64-unknown-linux-gnu/release/ehub_debug_consumer /app/ehub_debug_consumer

# Create non-root user for security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["/app/ehub_debug_consumer"]
