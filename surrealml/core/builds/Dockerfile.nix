FROM nixos/nix:latest@sha256:4e211f6763c542b08e9cdba448381286a3638903359390b46eab5f43ce6a6ed1

RUN nix-channel --update

RUN nix-env -p /nix/var/nix/profiles/container-dev -iA \
    nixpkgs.rustup \
    nixpkgs.gcc \
    nixpkgs.pkg-config \
    nixpkgs.cmake \
    nixpkgs.coreutils \
    nixpkgs.shadow \
    && chmod -R a+rX /nix/var/nix/profiles/container-dev

ENV PATH="/nix/var/nix/profiles/container-dev/bin:/nix/var/nix/profiles/container-dev/sbin:${PATH}"

RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY --chown=appuser:appuser . .

RUN mkdir -p /app/output && chown appuser:appuser /app/output

ENV HOME=/home/appuser

USER appuser

RUN rustup default stable

ENV PATH="/home/appuser/.cargo/bin:${PATH}"

CMD ["cargo", "run"]
