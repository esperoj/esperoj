import httpx
from esperoj.logging import getLogger
import subprocess
from os import getenv
from typing import Literal

logger = getLogger(__name__)

def run_command(
    host: Literal[
        "local", "github", "blacksmith", "blacksmith-arm", "codeberg", "cezeri", "framagit", "gitlab"
    ] = "local",
    command: str = "uptime",
) -> None:
    if host == "local":
        subprocess.run(["bash", "-lc", command], check=True)
    elif host in ["github", "blacksmith", "blacksmith-arm"]:
        runner = "ubuntu-latest" if host == "github" else host
        content = {"ref": "main", "inputs": {"runner": runner, "command": command}}

        with httpx.Client(http2=True, timeout=10.0) as client:
            response = client.post(
                "https://api.github.com/repos/esperoj/dotfiles/actions/workflows/run-command.yml/dispatches",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {getenv('GITHUB_TOKEN')}",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                data=content,
            )

        if response.status_code == 204:
            logger.info(
                "Succeed triggered. Visit https://github.com/esperoj/dotfiles/actions/workflows/run-command.yml"
            )
        else:
            logger.error(f"Failed with status code: {response.status_code}")
    elif host in ["codeberg", "cezeri"]:
        server, repo_id, token = {
            "codeberg": ("ci.codeberg.org", 12554, getenv("WOODPECKER_TOKEN")),
            "cezeri": ("build.cezeri.tech", 9, getenv("CEZERI_WOODPECKER_TOKEN")),
        }[host]

        content = {"branch": "main", "variables": {"WORKFLOW": "run-command", "COMMAND": command}}

        with httpx.Client(http2=True, timeout=10.0) as client:
            response = client.post(
                f"https://{server}/api/repos/{repo_id}/pipelines",
                headers={"Authorization": f"Bearer {token}", "Content-type": "application/json"},
                data=content,
            )

        result = response.json()
        number = result.get("number")
        logger.info(f"https://{server}/repos/{repo_id}/pipeline/{number}")
    elif host in ["framagit", "gitlab"]:
        server, project_id, token = {
            "gitlab": ("https://gitlab.com", 58158450, getenv("GITLAB_DOTFILES_TRIGGER_TOKEN")),
            "framagit": ("https://framagit.org", 108057, getenv("FRAMAGIT_DOTFILES_TRIGGER_TOKEN")),
        }[host]

        with httpx.Client(http2=True, timeout=10.0) as client:
            response = client.post(
                f"{server}/api/v4/projects/{project_id}/trigger/pipeline",
                data={
                    "token": token,
                    "ref": "main",
                    "variables[WORKFLOW]": "run-command",
                    "variables[COMMAND]": command,
                },
            )

        result = response.json()
        logger.info(result.get("web_url"))
