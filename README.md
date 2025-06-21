[![Coverage Status](https://coveralls.io/repos/github/aidh-ms/OpenICU/badge.svg?branch=main)](https://coveralls.io/github/aidh-ms/OpenICU?branch=main)

# Python Project Template

This is a project to unify and extract data from different ICU data sources.

## Features

- TODO

## Getting Started

> [!NOTE]
> Use the included dev container to automatically install all the necessary dev tools and dependencies.

1. **Clone the repository:**
    ```sh
    git clone https://github.com/Paul-B98/python-project-template.git
    cd python-project-template
    ```

2. **Open the project in Visual Studio Code:**
    ```sh
    code .
    ```

3. **Reopen in Container:**
    - Press `F1` to open the command palette.
    - Type `Remote-Containers: Reopen in Container` and select it.
    - VS Code will build the Docker container defined in the `.devcontainer` folder and open the project inside the container.

## Contributing

### Conventional Commits

We follow the [Conventional Commits]() specification to maintain a consistent commit history and enable automated tooling for releases and changelogs.

#### Commit message format:
```
Commit Message Format

<type>(optional scope): <short summary>

[optional body]

[optional footer(s)]
```

#### Common Types

- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (formatting, missing semicolons, etc.)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding or correcting tests
- `chore`: Changes to the build process or auxiliary tools
- `infra`: infrastructure ch

## Documentation

The following documentation describes the use and architecture of the OpenICU project.

### Requirements and architecture documentation

This project uses [arc42](https://docs.arc42.org/home/) to document the requirements and architecture:
1. [Introduction and Goals](docs/arc/introduction.md)
1. [Architecture Constraints](docs/arc/constraints.md)
1. [Context and Scope](docs/arc/context.md)
1. [Solution Strategy](docs/arc/strategy.md)
1. [Building Block View](docs/arc/building_block.md)
1. [Runtime View](docs/arc/runtime.md)
1. [Deployment view](docs/arc/deployment.md)
1. [Crosscutting Concepts](docs/arc/concepts.md)
1. [Architecture Decisions](docs/arc/decisions.md)
1. [Quality Requirements](docs/arc/quality.md)
1. [Risks and Technical Debt](docs/arc/risks.md)
1. [Glossary](docs/arc/glossary.md)
