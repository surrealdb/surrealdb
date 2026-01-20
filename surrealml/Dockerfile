# Use an official Rust image
FROM rust:1.88-slim

# Install necessary tools
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    libssl-dev \
    pkg-config \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    vim \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y python3 python3-pip

# Set the working directory
WORKDIR /app

# Copy the project files into the container
COPY . .

# Clean and build the Rust project
RUN cargo clean
RUN cargo build
RUN cp ./target/debug/libc_wrapper.so modules/c-wrapper/tests/test_utils/libc_wrapper.so

# Run the tests
CMD ["cargo", "test"]
