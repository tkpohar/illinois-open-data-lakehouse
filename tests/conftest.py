import logging

import pytest


@pytest.fixture(autouse=True)
def _restore_root_logger():
    """Undo handlers/levels installed by configure_logging (e.g. via cli.main)."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)
