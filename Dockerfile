# Use the distroless/cc:latest image as the base image
FROM gcr.io/distroless/cc:latest

# Declare a build-time argument for the target architecture
ARG TARGETARCH

# Add the binary file 'surreal' from the specified path on the host machine to the root directory in the container
ADD $TARGETARCH/surreal /

# Set the entry point for the container to be the 'surreal' binary
ENTRYPOINT ["/surreal"]
