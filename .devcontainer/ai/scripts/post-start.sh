#!/usr/bin/env bash

set -euo pipefail

uv sync --all-groups --locked --link-mode=copy
