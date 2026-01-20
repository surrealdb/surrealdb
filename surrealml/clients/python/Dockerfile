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
# you need these environment variables to be able to link the onnxruntime to the c-lib
ENV ORT_LIB_LOCATION=/onnxruntime/lib
ENV LD_LIBRARY_PATH=$ORT_LIB_LOCATION:$LD_LIBRARY_PATH


#  install python
RUN apt-get update && apt-get install -y python3 python3-pip
RUN apt install -y python3.11-venv
RUN python3 -m venv venv
RUN source venv/bin/activate && cd clients/python && pip install .

CMD ["tail", "-f", "/dev/null"]

