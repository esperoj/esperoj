FROM gitpod/workspace-python-3.12
ENV MACHINE_NAME="container"
RUN apt-get update ; \
    curl -fsLS https://codeberg.org/esperoj/dotfiles/raw/branch/main/bin/install.sh | bash -s -- dotfiles ; \
    ~/bin/setup.sh docker_gitpod ; \
    sudo rm -rf ~/.cache /var/lib/apt/lists /var/cache/apt/archives
