# More information about this image can be found at https://www.chainguard.dev/chainguard-images
FROM cgr.dev/chainguard/wolfi-base

RUN apk add libstdc++

# Declare a build-time argument for the target architecture
ARG TARGETARCH

# Add the binary file 'surreal' from the specified path on the host machine to the root directory in the container
ADD $TARGETARCH/surreal /

# Add the 'surreal' user to the container
RUN adduser -D surreal

# Set default data directory
ENV SURREAL_DATA=/data

COPY <<EOF /surreal-entrypoint.sh
#!/bin/sh

set -e

#
# Run the process as `surreal` user
#
# Change ownership of the data directory to surreal
if [ "\$(id -u)" = '0' ]; then
    find "\$SURREAL_DATA" \! -user surreal -exec chown surreal '{}' +

    # then restart script as surreal user
    exec su - surreal -c "\$0 \$*"
fi

exec /surreal "\$@"
EOF
RUN chmod +x /surreal-entrypoint.sh

ENTRYPOINT ["/surreal-entrypoint.sh"]
