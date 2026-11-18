import ast
import sys
from pathlib import Path

import adops.application

APPLICATION = Path(adops.application.__file__).parent
BANNED_STDLIB = {"os", "sys", "io", "pathlib", "subprocess", "socket", "sqlite3"}


def imported_modules(source: Path) -> set[str]:
    modules = set()
    for node in ast.walk(ast.parse(source.read_text())):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            modules.add(node.module or "")
    return modules


def test_application_imports_only_stdlib_domain_and_itself():
    offenders = {}
    for source in APPLICATION.glob("*.py"):
        for module in imported_modules(source):
            top = module.split(".")[0]
            allowed = (
                module.startswith(("adops.domain", "adops.application"))
                or (top in sys.stdlib_module_names and top not in BANNED_STDLIB)
            )
            if not allowed:
                offenders.setdefault(source.name, []).append(module)

    assert offenders == {}
