# Use an official Rust image
FROM rust:1.83-slim@sha256:540c902e99c384163b688bbd8b5b8520e94e7731b27f7bd0eaa56ae1960627ab

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
