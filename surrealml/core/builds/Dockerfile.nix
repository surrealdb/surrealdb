# Start from a base image, e.g., Ubuntu
FROM nixos/nix:latest

# Update Nix channel
RUN nix-channel --update

# Install Rust and build tools using Nix
RUN nix-env -iA nixpkgs.rustup nixpkgs.gcc nixpkgs.pkg-config nixpkgs.cmake nixpkgs.coreutils

# Initialize Rust environment
RUN rustup default stable

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY . .

# RUN cargo build --release

CMD ["cargo", "run"]
