FROM rust:1.73-buster

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y locales && \
    # needed for aws-lc (transitive dependency of rustls)
    # https://aws.github.io/aws-lc-rs/requirements/linux.html
    apt-get install -y cmake gcc g++ libclang1 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
 && rm -rf /var/lib/apt/lists/*
ENV LANG=en_US.UTF-8

# Install our own CAs on the image.
# Assumes Linux Debian based image.
COPY CAs/* /usr/local/share/ca-certificates/
# Store custom CAs somewhere where the backend can find them later.
COPY CustomCAs/* /usr/local/share/custom-ca-certificates/
RUN update-ca-certificates
