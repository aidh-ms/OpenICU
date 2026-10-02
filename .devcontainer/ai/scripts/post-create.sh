#!/usr/bin/env bash

set -euo pipefail

mkdir -p "$HOME/.local/bin"

if ! grep -q 'HOME/.local/bin' "$HOME/.bashrc"; then
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
fi

export PATH="$HOME/.local/bin:$PATH"


# Claude Code
npm install -g @anthropic-ai/claude-code

# OpenICU development tooling
python -m pip install --user -r .devcontainer/ai/requirements.txt


echo
echo "Claude Code:"
claude --version
