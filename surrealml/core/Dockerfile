# Use an official Rust image
FROM rust:1.83-slim

# Install necessary tools
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the project files into the container
COPY . .

# Set the ONNX Runtime library path
ENV ORT_LIB_LOCATION=/onnxruntime/lib
ENV LD_LIBRARY_PATH=$ORT_LIB_LOCATION:$LD_LIBRARY_PATH

# Clean and build the Rust project
RUN cargo clean
RUN cargo build --features tensorflow-tests

# Run the tests
CMD ["cargo", "test", "--features", "tensorflow-tests"]
