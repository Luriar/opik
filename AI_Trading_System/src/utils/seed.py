"""Random seed utilities."""

from __future__ import annotations

import random
from typing import Any

import numpy as np

from src.utils.config_loader import ConfigError, load_yaml_config


DEFAULT_RANDOM_SEED = 42


def get_random_seed(config: dict[str, Any] | None = None) -> int:
    """Return the configured random seed, falling back to 42."""
    if config is None:
        try:
            config = load_yaml_config("configs/model.yaml")
        except ConfigError:
            return DEFAULT_RANDOM_SEED

    value = (
        config.get("model", {})
        .get("common", {})
        .get("random_seed", DEFAULT_RANDOM_SEED)
    )
    return int(value)


def set_random_seed(seed: int | None = None) -> int:
    """Set Python and NumPy random seeds and return the seed used."""
    seed_value = DEFAULT_RANDOM_SEED if seed is None else int(seed)
    random.seed(seed_value)
    np.random.seed(seed_value)
    return seed_value


def set_seed_from_config(config: dict[str, Any] | None = None) -> int:
    """Set random seeds using model config when available."""
    seed = get_random_seed(config)
    return set_random_seed(seed)
