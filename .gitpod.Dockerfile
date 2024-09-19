FROM gitpod/workspace-python-3.12
ENV MACHINE_NAME="container"
RUN curl -fsLS https://codeberg.org/esperoj/dotfiles/raw/branch/main/bin/install.sh | bash -s -- dotfiles ; \
    sudo ~/bin/setup.sh docker_gitpod ; \
    rm -rf ~/.cache /var/lib/apt/lists /var/cache/apt/archives
