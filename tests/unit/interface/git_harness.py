import os
import subprocess

GIT_ENV = {
    **os.environ,
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_AUTHOR_NAME": "adops",
    "GIT_AUTHOR_EMAIL": "adops@example.com",
    "GIT_COMMITTER_NAME": "adops",
    "GIT_COMMITTER_EMAIL": "adops@example.com",
}


def git(*args, cwd):
    result = subprocess.run(
        ["git", *args], cwd=cwd, env=GIT_ENV, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()
