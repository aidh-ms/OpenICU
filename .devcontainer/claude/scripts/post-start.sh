#!/usr/bin/env bash
set -euo pipefail

sudo chown -R vscode:vscode /home/vscode/.claude
chmod 700 /home/vscode/.claude

uv sync --all-groups
