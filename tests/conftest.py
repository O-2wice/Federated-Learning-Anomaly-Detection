"""Shared test helpers: import pipeline scripts (some have numeric file names)."""
import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))


def load_script(filename: str, module_name: str):
    """Import scripts/<filename> as `module_name` (e.g. 04_federated_aggregation.py)."""
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def aggregation():
    return load_script("04_federated_aggregation.py", "federated_aggregation")


@pytest.fixture(scope="session")
def producer_module():
    return load_script("02_kafka_producer.py", "kafka_producer")
