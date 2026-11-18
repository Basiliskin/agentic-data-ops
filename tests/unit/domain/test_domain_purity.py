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


CLOCK_READS = {"now", "utcnow", "today"}


def clock_reads(source: str) -> list[str]:
    """Calls like `datetime.now()` or `dt.utcnow()`, whatever the receiver is named."""
    return [
        ast.unparse(node.func)
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in CLOCK_READS
    ]


def test_domain_never_reads_the_clock():
    offenders = {
        source.name: reads
        for source in DOMAIN.glob("*.py")
        if (reads := clock_reads(source.read_text()))
    }

    assert offenders == {}


def test_clock_reads_are_found_under_any_alias():
    source = """
from datetime import datetime, date
from datetime import datetime as dt
import datetime as d
a = datetime.now(UTC)
b = dt.utcnow()
c = d.datetime.now()
e = date.today()
f = now()
"""

    assert clock_reads(source) == ["datetime.now", "dt.utcnow", "d.datetime.now", "date.today"]
