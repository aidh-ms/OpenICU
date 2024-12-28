# Python Project Template

This is a template for creating Python projects. It includes a basic project structure, configuration files, and setup scripts to help you get started quickly.

## Features

- 📁 Pre-configured with `poetry` for dependency management
- 🪛 Linting with `ruff`
- 🔧 Type checking with `mypy`
- 🧪 Testing with `pytest`
- 📝 Pre-commit hooks for code quality
- 📦 Uses `dev container` for development setup

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
