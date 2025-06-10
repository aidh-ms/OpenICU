# Contributing

Here is how you can contribute to this project.

## Setup

Use the included dev container to automatically install all the necessary dev tools and dependencies.

> **Prerequisite**: To use this you first need to install docker under Linux, MacOS or WSL2 under windows.

1. **Clone the repository:**
    ```bash
    git clone git+https://github.com/aidh-ms/pyAKI
    cd pyAKI
    ```

2. **Open the project in Visual Studio Code:**
    ```bash
    code .
    ```

3. **Reopen in Container:**
    - Press `F1` to open the command palette.
    - Type `Remote-Containers: Reopen in Container` and select it.
    - VS Code will build the Docker container defined in the `.devcontainer` folder and open the project inside the container.

## Development



## Testing

To test your contribution, you can use the testing tap in the VS code or utilise the following command to run the unit tests for this project:

```shell
poetry run pytest .
```
