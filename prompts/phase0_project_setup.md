# prompts/phase0_project_setup.md

```text
# ==========================================================
# AI Trading System v1.0
#
# Phase 0 : Project Setup / Infrastructure
# ==========================================================

Implement Phase 0 ONLY.

Before coding, read:

README.md
AGENTS.md
IMPLEMENTATION_GUIDE.md
CODING_STANDARDS.md
PROJECT_TASKS.md
PROJECT_STATUS.md

Reference:

pyproject.toml
requirements.txt
configs/
tests/test_config.py
tests/test_project_structure.py
tests/test_project_health.py

------------------------------------------------------------

Implement

src/utils/

    config_loader.py
    logger.py
    paths.py
    seed.py
    version.py

Also ensure package structure:

src/
    __init__.py

src/data/
    __init__.py

src/features/
    __init__.py

src/models/
    __init__.py

src/validation/
    __init__.py

src/portfolio/
    __init__.py

src/backtest/
    __init__.py

src/execution/
    __init__.py

src/utils/
    __init__.py

------------------------------------------------------------

Requirements

Create or validate required directories:

docs/
configs/
tests/
src/
outputs/
notebooks/
data/
logs/

Config Loader:

- Load YAML files from configs/
- Validate required config files exist
- Return dictionary
- Raise clear error if config is missing or invalid

Logger:

- Create reusable project logger
- Include timestamp, run_id, step, status, message
- Do not silently ignore exceptions

Path Manager:

- Define project root
- Define data, outputs, logs, configs paths
- Create missing runtime directories if needed

Seed Manager:

- Set Python random seed
- Set NumPy random seed
- Use seed from configs/model.yaml if available
- Default seed = 42

Version Manager:

- Return project version
- Return config version if available
- Return model/feature version if available

------------------------------------------------------------

Rules

- Follow CODING_STANDARDS.md
- Use type hints
- Use docstrings
- No hardcoding except safe defaults
- Config driven where possible
- Do not implement Data Layer, Feature Engine, Model, Portfolio, Backtest, or Execution logic
- Do not modify docs unless necessary
- Do not implement future phases

------------------------------------------------------------

Run

pytest \
tests/test_config.py \
tests/test_project_structure.py \
tests/test_project_health.py

Fix every failure.

------------------------------------------------------------

Update PROJECT_STATUS.md

Mark Phase 0 as complete only if:

- Config files load successfully
- Required project structure exists
- Required health checks pass
- All Phase 0 tests pass

Do NOT implement future phases.
```
