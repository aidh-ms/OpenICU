# OpenICU AI Isolated Environment

This Claude Code session runs inside an isolated development container.

## Security boundary

Workspace:

    /workspaces/OpenICU

Real ICU datasets and host data are intentionally unavailable.

- Work only with files available inside `/workspaces/OpenICU`.
- Never attempt to bypass the container isolation.
- Never attempt to discover or mount host directories.
- Never attempt to access host Docker.
- Never attempt to access host SSH, GPG, credentials, or local host services.
- Never request real patient or ICU data.
- Never add real datasets to this workspace.

`.git`, `.devcontainer`, and `.claude` are intentionally read-only inside the container.

Do not attempt to modify:
- `.git`
- `.devcontainer`
- `.claude`
- Docker mounts
- Docker sockets
- host-side lifecycle hooks

The user performs commits, branch changes, rebases, and pushes on the host.

## Data-dependent work

If a task requires real dataset behavior:

- inspect the implementation and schemas,
- use synthetic or public fixtures,
- construct synthetic tests when useful,
- state clearly what the user must validate manually against the real dataset.

## Development workflow

Before changing code:

1. Inspect the relevant implementation.
2. Trace callers and dependencies.
3. Understand the existing OpenICU behavior.
4. Make the smallest relevant change.
5. Run the narrowest applicable tests first.

Do not refactor unrelated code.

Prefer repository evidence over assumptions.

## Python

The project uses `uv`.

Synchronize dependencies with:

    uv sync --all-groups --locked --link-mode=copy

Prefer:

    uv run ...

for project commands where appropriate.
