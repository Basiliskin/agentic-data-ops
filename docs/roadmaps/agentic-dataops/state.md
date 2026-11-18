# State

- 2026-11-17 | horizon 1 | planned | 7 phases (demo shop loads end-to-end); gate passed after heal (1 blocker, 2 majors fixed)
- 2026-11-17 | horizon 1 | replanned | revise-phases, 6 phases revised — built-in deterministic Checks on every Sync (ADR-0013) and adops validate folded in
- 2026-11-18 | horizon 1 | completed | 7 phases, 1 amendment, 0 overrides — demo shop loads end-to-end via adops run; Docker E2E job in CI (no PR run yet)
- 2026-11-18 | horizon 2 | planned | 6 phases (Breaking Changes + accept-schema, user-declared Checks); gate passed after heal (0 blockers, 2 majors fixed, 1 phase split)
- 2026-11-18 | horizon 2 | completed | 6 phases | 2 amendments (accept-schema relaxes dropped NOT NULL columns; demo seed email nullable) | 0 overrides
