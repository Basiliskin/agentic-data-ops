from importlib.metadata import version

import adops


def test_version_matches_installed_distribution():
    assert adops.__version__ == version("adops")
