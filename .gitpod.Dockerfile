FROM gitpod/workspace-python-3.11
ENV MACHINE_NAME="container"

RUN curl -fsLS https://codeberg.org/esperoj/dotfiles/raw/branch/main/bin/install-dotfiles.sh | bash \
    && ~/bin/setup.sh \
    && sudo rm -r /var/lib/apt/lists /var/cache/apt/archives
