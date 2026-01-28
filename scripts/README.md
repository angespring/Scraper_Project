Environment Setup

This project uses the OpenAI API for assisted workflows during development. An API key is required to run features that rely on OpenAI models.

Required Environment Variable

The following environment variable must be set:

OPENAI_API_KEY


This key is not stored in the repository and should never be committed to source control.

How to Set the API Key (macOS / zsh)

Add the following line to your shell configuration file, such as ~/.zshrc:

export OPENAI_API_KEY="your_api_key_here"


Then reload your shell:

source ~/.zshrc


If you are using VS Code, launch it from the terminal so it inherits the environment variable:

code .

VS Code Alternative