import ast
import sys
from pathlib import Path

import adops.domain

DOMAIN = Path(adops.domain.__file__).parent
ALLOWED_THIRD_PARTY = {"pydantic", "yaml"}
BANNED_STDLIB = {"os", "sys", "io", "pathlib", "random", "time", "subprocess", "socket", "sqlite3"}


def imported_modules(source: Path) -> set[str]:
    modules = set()
    for node in ast.walk(ast.parse(source.read_text())):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            modules.add(node.module or "")
    return modules


def test_domain_imports_only_stdlib_pydantic_yaml_and_itself():
    offenders = {}
    for source in DOMAIN.glob("*.py"):
        for module in imported_modules(source):
            top = module.split(".")[0]
            allowed = (
                module.startswith("adops.domain")
                or top in ALLOWED_THIRD_PARTY
                or (top in sys.stdlib_module_names and top not in BANNED_STDLIB)
            )
            if not allowed:
                offenders.setdefault(source.name, []).append(module)

    assert offenders == {}
