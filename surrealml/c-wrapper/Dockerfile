# Use an official Rust image
FROM rust:1.83-slim

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

# Set the working directory
WORKDIR /app

# Copy the project files into the container
COPY . .

# install python for the tests
RUN apt-get update && apt-get install -y python3 python3-pip

RUN cd c-wrapper && cargo build --target-dir target && bash scripts/copy_over_lib_docker.sh

# Run the tests
CMD ["bash", "c-wrapper/scripts/run_tests.sh"]
