# Issues

## [Potential] Update Scripts Mechanism

**Description:**
Enable users to update their local project scripts (`scripts/driver.sh`, `scripts/fairway-hpc.sh`) to the latest versions provided by the installed `fairway` package. This addresses the need to update these scripts now that they live outside the package.

**Plan:**
Modify the `fairway eject` command to include support for ejecting/updating the operational scripts.

### Proposed Changes

1.  **Modify `src/fairway/cli.py`**:
    *   Update the `eject` command to check for the `scripts/` directory.
    *   Prompt the user to overwrite `scripts/driver.sh` and `scripts/fairway-hpc.sh` if they exist.
    *   Write the latest content from `fairway.templates` to these files.
    *   Ensure the scripts are made executable (`chmod +x`).

**Verification:**
*   Create a temporary test project.
*   Modify local scripts.
*   Run `fairway eject` and confirm overwrite prompt and successful update.
