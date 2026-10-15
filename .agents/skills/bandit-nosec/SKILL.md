---
name: Bandit nosec conventions
description: How to phrase, place, and verify bandit `# nosec` suppressions, and the f-string brace pitfall when writing SQL strings. Use when resolving bandit findings (B608 SQL, B603/B607/B404 subprocess, B101 asserts, B104 bind-all, B105 hardcoded password, B110 try/except/pass), adding or reviewing `# nosec`, or merging/concatenating SQL string fragments.
---

# Bandit `# nosec` conventions

## Core rule

Add `# nosec` only after establishing the finding is a genuine false positive — i.e. the
flagged value cannot reach the dangerous slot. Otherwise **fix the code**.

When the value can come from an untrusted source (DB catalog, env var, request body),
prefer a **runtime guard** over suppression, e.g.:

```python
if not _CHANNEL_TABLE_RE.fullmatch(table):      # fail closed
    raise ValueError(f"Refusing to use non-canonical table {table!r}")
```

## Phrasing — put the reason BEFORE a second `#`

Bandit parses **everything after `# nosec`** as test tokens and logs a warning for every
word that is not an ID. A trailing reason therefore produces noise
(`Test in comment: argv is not a test name or id, ignoring`).

```python
subprocess.run(cmd)  # argv list, no shell # nosec B603          <- good
subprocess.run(cmd)  # nosec B603 - argv list, no shell          <- bad (warnings)
```

## Multiple IDs — space-separated, never commas

Bandit's token regex `(?:(B\d+|[a-z\d_]+),?)+` collapses a comma-separated list to only the
**last** id, silently under-suppressing.

```python
foo()  # argv list, no shell # nosec B603 B607   <- good
foo()  # nosec B603,B607                          <- bad (only B607 applies)
```

## Placement — on the exact reported line, next to the variable

Bandit reports the `Str` whose text matches its SQL regex (`select … from`,
`insert into … values`, `update … set`, `delete from`). With implicit concatenation that is
the **first** fragment, so put the nosec on the fragment that ALSO carries the interpolation:

```python
out = self._psql(
    f"SELECT tablename FROM pg_tables WHERE schemaname = '{DATA_NAMESPACE}' "  # DATA_NAMESPACE is a constant # nosec B608
    "AND tablename ~ '^channel_[0-9a-f]{32}$' "   # plain string → braces are literal
    "ORDER BY tablename",
)
```

Not on a variable-free prefix:

```python
    "SELECT tablename FROM pg_tables "   # BAD: no variable on this line
    f"WHERE schemaname = '{DATA_NAMESPACE}' "
```

For a multi-line triple-quoted f-string, bandit accepts a nosec anywhere inside the node's
span (e.g. the closing `"""`), but prefer restructuring to implicit concatenation so the
nosec sits beside the interpolation.

## f-string brace pitfall (silent bug)

`{` `}` in an f-string are **replacement fields**. Editing that folds a plain fragment
holding literal braces into an f-string evaluates them:

```python
# WRONG — {32} is a field → renders '^channel_[0-9a-f]32$' (matches "channel_a32", not 32 hex)
f"... tablename ~ '^channel_[0-9a-f]{32}$' ..."

# RIGHT — escape, or keep the literal-brace part as a plain fragment
f"... tablename ~ '^channel_[0-9a-f]{{32}}$' ..."
"AND tablename ~ '^channel_[0-9a-f]{32}$' "
```

This fails **silently** (no exception), and unit tests that mock the DB will not catch it.
After any SQL-string change, render and assert the actual string.

## Verify — findings AND parser warnings

```bash
uv run --no-sync bandit -r src scripts docker/app -f json -o /tmp/bandit.json
python3 -c "import json; print('findings:', len(json.load(open('/tmp/bandit.json'))['results']))"
uv run --no-sync bandit -r src scripts docker/app -f screen 2>&1 | grep -c "not a test name or id"  # must be 0
uv run --no-sync ruff check <files>
```

A green bandit run is **not** proof the SQL is correct — a corrupted regex still reports
0 findings.

## Rule cheat-sheet

| Rule | Meaning | Typical resolution |
| ---- | ------- | ------------------ |
| B101 | `assert` | real guard, or nosec behind `if __name__ == "__main__":` |
| B104 | hardcoded bind-all | nosec when `0.0.0.0` is intended (containers) |
| B105 | hardcoded password | nosec when the string is not a secret (token type, env var name) |
| B110 | `try/except/pass` | `contextlib.suppress(...)` |
| B404 | `import subprocess` | nosec with rationale when shelling out intentionally |
| B603 | subprocess without `shell=True` | nosec — the argv-list form is the safe pattern |
| B607 | partial executable path | fix via `shutil.which(...)`, or nosec when PATH is image-controlled |
| B608 | SQL built via f-string/format | validate the identifier, or nosec when only constants are interpolated |
