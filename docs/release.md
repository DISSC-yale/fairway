# Release Process

This repository uses GitHub Actions to automate the release process.

## Triggers

The automated workflow is triggered when a **Release** is published in GitHub.

## What Happens

1.  **Container Build**: The Docker image is built from the `Dockerfile` in the root of the repository.
2.  **Container Push**: The image is pushed to the GitHub Container Registry (GHCR) at `ghcr.io/dissc-yale/fairway`.
3.  **Versioning**: The image is tagged with:
    *   The release tag (e.g., `v1.0.1`)
    *   `latest` (if it is the default branch)

## How to Release

1.  Update the version number in `pyproject.toml`.
2.  Commit your changes: `git commit -am "Bump version to x.y.z"`.
3.  Push to main: `git push origin main`.
4.  Go to the repository on GitHub -> **Releases** -> **Draft a new release**.
5.  Choose a tag (e.g., `v1.0.1`).
6.  Click **Publish release**.

The [Release Workflow](https://github.com/DISSC-yale/fairway/actions/workflows/release.yml) will start automatically.
