# Use ChainGuard's glibc-dynamic image as the base image. More information about this image can be found at https://www.chainguard.dev/chainguard-images
FROM cgr.dev/chainguard/glibc-dynamic

# Declare a build-time argument for the target architecture
ARG TARGETARCH

# Add the binary file 'surreal' from the specified path on the host machine to the root directory in the container
ADD $TARGETARCH/surreal /

# Set the entry point for the container to be the 'surreal' binary
ENTRYPOINT ["/surreal"]
