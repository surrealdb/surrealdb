FROM alpine:latest

# Install

ADD gui .

ADD surreal .

# Expose the necessary ports

EXPOSE 8000 33693

# Set the default command

ENTRYPOINT ["/contributors"]