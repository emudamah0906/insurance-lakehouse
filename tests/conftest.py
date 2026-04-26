import pytest


@pytest.fixture(scope="session")
def sample_size():
    """Small row count for fast unit tests — not the full 500K synthetic set."""
    return 1_000
