#!/usr/bin/env bash
set -euo pipefail

# Git bash completion
echo "source /usr/share/bash-completion/completions/git" >> ~/.bashrc

# Install development dependencies
pip install -r .devcontainer/claude/requirements.txt

