FROM gliderlabs/alpine:latest

MAINTAINER Tobie Morgan Hitchcock <tobie@abcum.com>

# Install

ADD gui .

ADD surreal .

# Expose the necessary ports

EXPOSE 8000 33693

# Set the default command

CMD echo $(ip route | awk '/default/ { print $3 }') docker >> /etc/hosts && surreal