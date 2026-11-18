from collections.abc import Iterable, Mapping

from adops.domain.secret_reference import SecretReference


class MissingSecretError(Exception):
    """Secret References whose environment variables are unset or empty; names only."""

    def __init__(self, names: Iterable[str]) -> None:
        self.names = tuple(sorted(set(names)))
        super().__init__(
            "these environment variables are not set or are empty: " + ", ".join(self.names)
        )


def resolve_secret_references(
    env: Mapping[str, str], refs: Iterable[SecretReference]
) -> dict[str, str]:
    """Resolve every reference at once; an empty value counts as missing."""
    names = {ref.name for ref in refs}
    missing = [name for name in names if not env.get(name)]
    if missing:
        raise MissingSecretError(missing)
    return {name: env[name] for name in names}
