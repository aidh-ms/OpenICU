# Contributing

Here is how you can contribute to this project. Contributions of dataset configurations and concept definitions are especially welcome — they are pure YAML and require no changes to the framework itself.

## Setup

Use the included dev container to automatically install all the necessary dev tools and dependencies.

> **Prerequisite**: To use this you first need to install Docker under Linux, macOS, or WSL2 under Windows.

1. **Clone the repository:**
    ```bash
    git clone https://github.com/aidh-ms/OpenICU.git
    cd OpenICU
    ```

2. **Open the project in Visual Studio Code:**
    ```bash
    code .
    ```

3. **Reopen in Container:**
    - Press `F1` to open the command palette.
    - Type `Remote-Containers: Reopen in Container` and select it.
    - VS Code will build the Docker container defined in the `.devcontainer` folder and open the project inside the container.

Alternatively, set up a local environment with [uv](https://docs.astral.sh/uv/):

```bash
uv sync --all-groups
```

## Development with Conventional Commits

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification to maintain a consistent commit history and enable automated tooling for releases and changelogs.

### Commit message format
```
<type>(optional scope): <short summary>

[optional body]

[optional footer(s)]
```

### Common Types

- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (formatting, missing semicolons, etc.)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding or correcting tests
- `chore`: Changes to the build process or auxiliary tools
- `infra`: Infrastructure changes

## Code quality

The CI pipeline runs linting, type checking, and tests on every push. Run the same checks locally before opening a pull request:

```bash
uv run ruff format        # format code
uv run ruff check         # lint (add --fix to auto-fix)
uv run ty check .         # static type checking
uv run pytest             # tests with coverage
```

Code style follows the repository's Ruff configuration (line length 120) and all public functions require type hints.

## Testing

To test your contribution, you can use the testing tab in VS Code or run the unit tests directly:

```shell
uv run pytest .
```

## Contributing dataset or concept configurations

- New **dataset support** lives in `config/datasets/<dataset>/<version>/tables/` (one YAML per source table). See the [extraction configuration guide](../user_guide/extraction.md).
- New **concepts** consist of a dataset-agnostic definition in `config/concepts/<category>/` plus one mapping per dataset in `config/datasets/<dataset>/<version>/mappings/`. See the [concept configuration guide](../user_guide/concepts.md).

When adding concept mappings, please document the source items (e.g. MIMIC `itemid`s) you considered, so reviewers can verify completeness.
