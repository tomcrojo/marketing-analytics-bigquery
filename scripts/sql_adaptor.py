"""Convert BigQuery SQL to PostgreSQL-compatible SQL.

Handles the most common BigQuery-specific syntax that differs from PostgreSQL:
- SAFE_DIVIDE(a, b)        -> COALESCE(a::float / NULLIF(b, 0), 0)
- GENERATE_UUID()          -> gen_random_uuid()
- DATE_DIFF(end, start, unit) -> EXTRACT(unit FROM (end - start))
- TIMESTAMP_DIFF(ts1, ts2, unit) -> EXTRACT(EPOCH FROM (ts1 - ts2)) / divisor
- DATE_TRUNC(expr, MONTH)  -> DATE_TRUNC('month', expr)
- TIMESTAMP_TRUNC(ts, WEEK(MONDAY)) -> DATE_TRUNC('week', ts)
- INT64 / FLOAT64 / STRING -> INTEGER / DOUBLE PRECISION / TEXT
- CREATE OR REPLACE TABLE  -> DROP TABLE IF EXISTS ... CASCADE; CREATE TABLE ...
- Backtick quoting         -> removed (PG uses unquoted identifiers)
- TABLE DDL OPTIONS(...)   -> removed
- TABLE DDL PARTITION BY   -> removed
- TABLE DDL CLUSTER BY     -> removed
- COUNTIF(cond)            -> COUNT(*) FILTER (WHERE cond)
- NULLS LAST               -> removed (PG default)
"""

import re


def bq_to_pg(sql: str) -> str:
    """Convert a BigQuery SQL string to PostgreSQL-compatible SQL."""
    result = sql

    # ── Remove PROJECT_ID namespace prefix ──
    result = re.sub(r"\$\{PROJECT_ID\}\.", "", result)

    # ── Remove backtick quoting (PostgreSQL doesn't use backticks) ──
    result = result.replace("`", "")

    # ── SAFE_DIVIDE(numerator, denominator) ──
    # Repeat until no more matches (handles nested SAFE_DIVIDE)
    for _ in range(10):
        new_result = _replace_safe_divide(result)
        if new_result == result:
            break
        result = new_result

    # ── GENERATE_UUID() ──
    result = result.replace("GENERATE_UUID()", "gen_random_uuid()")

    # ── DATE_DIFF(end, start, DAY) ──
    result = re.sub(
        r"DATE_DIFF\s*\(\s*(.+?),\s*(.+?),\s*DAY\s*\)",
        r"EXTRACT(DAY FROM (\1 - \2))",
        result,
    )

    # ── TIMESTAMP_DIFF(ts1, ts2, SECOND) ──
    result = re.sub(
        r"TIMESTAMP_DIFF\s*\(\s*(.+?),\s*(.+?),\s*SECOND\s*\)",
        r"EXTRACT(EPOCH FROM (\1 - \2))",
        result,
    )

    # ── TIMESTAMP_DIFF(ts1, ts2, MINUTE) ──
    result = re.sub(
        r"TIMESTAMP_DIFF\s*\(\s*(.+?),\s*(.+?),\s*MINUTE\s*\)",
        r"(EXTRACT(EPOCH FROM (\1 - \2)) / 60)",
        result,
    )

    # ── DATE_TRUNC(expr, MONTH) → DATE_TRUNC('month', expr) ──
    result = re.sub(
        r"DATE_TRUNC\s*\(\s*(.+?),\s*MONTH\s*\)",
        r"DATE_TRUNC('month', \1)",
        result,
    )

    # ── TIMESTAMP_TRUNC(ts, WEEK(MONDAY)) → DATE_TRUNC('week', ts) ──
    result = re.sub(
        r"TIMESTAMP_TRUNC\s*\(\s*(.+?),\s*WEEK\(MONDAY\)\s*\)",
        r"DATE_TRUNC('week', \1)",
        result,
    )

    # ── Data type conversions ──
    result = re.sub(r"\bINT64\b", "INTEGER", result)
    result = re.sub(r"\bFLOAT64\b", "DOUBLE PRECISION", result)
    result = re.sub(r"\bSTRING\b", "TEXT", result)

    # ── Remove column-level OPTIONS(description="...") ──
    result = _remove_options_calls(result)

    # ── Remove DDL-level OPTIONS, PARTITION BY, CLUSTER BY (before column list) ──
    result = _clean_ddl_header_clauses(result)

    # ── Remove trailing PARTITION BY / CLUSTER BY after column list close paren ──
    result = _clean_ddl_trailing_clauses(result)

    # ── CREATE OR REPLACE TABLE → DROP + CREATE ──
    result = re.sub(
        r"CREATE\s+OR\s+REPLACE\s+TABLE\s+(\S+)",
        _drop_create_replace,
        result,
        flags=re.IGNORECASE,
    )

    # ── COUNTIF(cond) → COUNT(*) FILTER (WHERE cond) ──
    result = _replace_countif(result)

    # ── Remove NULLS LAST (PG defaults to this) ──
    result = re.sub(r"\s+NULLS\s+LAST", "", result, flags=re.IGNORECASE)

    # ── Collapse multiple blank lines ──
    result = re.sub(r"\n{3,}", "\n\n", result)

    return result


def _drop_create_replace(match: re.Match) -> str:
    """Replace CREATE OR REPLACE TABLE with DROP IF EXISTS + CREATE TABLE."""
    table_name = match.group(1)
    return f"DROP TABLE IF EXISTS {table_name} CASCADE;\nCREATE TABLE {table_name}"


def _clean_ddl_header_clauses(sql: str) -> str:
    """Remove BigQuery DDL header clauses: OPTIONS, PARTITION BY, CLUSTER BY.

    These appear between 'CREATE TABLE name' and the opening '(' of column defs.
    Window function PARTITION BY inside (...) is NOT affected.
    """
    pattern = re.compile(
        r"(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+\S+\s*)"  # group 1: up to table name
        r"((?:"
        r"OPTIONS\s*\([^)]*(?:\([^)]*\)[^)]*)*\)\s*"  # OPTIONS with nested parens
        r"|PARTITION\s+BY\s+\S+\s*"  # PARTITION BY col
        r"|CLUSTER\s+BY\s+\S+(?:\s*,\s*\S+)*\s*"  # CLUSTER BY col, col
        r"|\s+"
        r")*)"  # zero or more of these clauses
        r"(\(|AS)",  # group 3: start of column defs or AS
        re.IGNORECASE,
    )

    def _strip_header(match: re.Match) -> str:
        prefix = match.group(1)
        terminator = match.group(3)
        return prefix + terminator

    return pattern.sub(_strip_header, sql)


def _clean_ddl_trailing_clauses(sql: str) -> str:
    """Remove trailing PARTITION BY / CLUSTER BY after CREATE TABLE column list.

    Handles the pattern:
        CREATE TABLE name (
          col1 TYPE,
          ...
        )
        PARTITION BY DATE(col)
        CLUSTER BY col1, col2
        ;

    Only removes from CREATE TABLE blocks (identified by tracking paren depth).
    """
    result = []
    i = 0
    in_create = False
    create_depth = 0  # paren depth relative to CREATE TABLE's column list

    while i < len(sql):
        # Detect start of CREATE TABLE
        m = re.match(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+\S+\s*\(", sql[i:], re.IGNORECASE
        )
        if m and not in_create:
            in_create = True
            create_depth = 0
            # Find the opening paren
            paren_pos = m.group(0).rfind("(")
            result.append(sql[i : i + paren_pos + 1])
            i += paren_pos + 1
            create_depth = 1
            continue

        if in_create:
            if sql[i] == "(":
                create_depth += 1
                result.append(sql[i])
                i += 1
            elif sql[i] == ")":
                create_depth -= 1
                result.append(sql[i])
                i += 1
                if create_depth == 0:
                    # Column list closed — now strip trailing PARTITION BY / CLUSTER BY
                    rest = sql[i:]
                    stripped = re.sub(
                        r"^\s*(?:PARTITION\s+BY\s+[^\n]+\n?|CLUSTER\s+BY\s+[^\n]+\n?)+",
                        "",
                        rest,
                        flags=re.IGNORECASE,
                    )
                    result.append(stripped[: len(stripped) - len(stripped.lstrip())])
                    i += len(rest) - len(stripped.lstrip())
                    in_create = False
            else:
                result.append(sql[i])
                i += 1
        else:
            result.append(sql[i])
            i += 1

    return "".join(result)


def _replace_safe_divide(sql: str) -> str:
    """Replace SAFE_DIVIDE(a, b) handling nested parentheses."""
    result = []
    i = 0
    pattern = "SAFE_DIVIDE"
    while i < len(sql):
        idx = sql.upper().find(pattern, i)
        if idx == -1:
            result.append(sql[i:])
            break
        result.append(sql[i:idx])
        paren_start = idx + len(pattern)
        while paren_start < len(sql) and sql[paren_start] in " \t\n":
            paren_start += 1
        if paren_start >= len(sql) or sql[paren_start] != "(":
            result.append(sql[idx:paren_start])
            i = paren_start
            continue

        depth = 0
        j = paren_start
        while j < len(sql):
            if sql[j] == "(":
                depth += 1
            elif sql[j] == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1

        inner = sql[paren_start + 1 : j]
        num, denom = _split_top_level(inner)
        pg_expr = f"COALESCE(({num.strip()})::float / NULLIF(({denom.strip()}), 0), 0)"
        result.append(pg_expr)
        i = j + 1

    return "".join(result)


def _split_top_level(s: str):
    """Split a string on top-level commas (not inside parens)."""
    depth = 0
    for idx, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            return s[:idx], s[idx + 1 :]
    return s, ""


def _remove_options_calls(sql: str) -> str:
    """Remove all OPTIONS(...) blocks from anywhere in the SQL.

    Handles nested parentheses inside OPTIONS (e.g. labels=[("k","v")]).
    """
    result = []
    i = 0
    pattern = "OPTIONS"
    while i < len(sql):
        idx = sql.upper().find(pattern, i)
        if idx == -1:
            result.append(sql[i:])
            break
        # Check it's a standalone word
        if idx > 0 and sql[idx - 1].isalpha():
            result.append(sql[i : idx + len(pattern)])
            i = idx + len(pattern)
            continue
        result.append(sql[i:idx])
        # Find opening paren
        paren_start = idx + len(pattern)
        while paren_start < len(sql) and sql[paren_start] in " \t\n":
            paren_start += 1
        if paren_start >= len(sql) or sql[paren_start] != "(":
            result.append(sql[idx:paren_start])
            i = paren_start
            continue
        # Find matching close paren (depth-aware)
        depth = 0
        j = paren_start
        while j < len(sql):
            if sql[j] == "(":
                depth += 1
            elif sql[j] == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        # Skip the OPTIONS(...) block entirely
        i = j + 1
    return "".join(result)


def _replace_countif(sql: str) -> str:
    """Replace COUNTIF(condition) with COUNT(*) FILTER (WHERE condition)."""
    result = []
    i = 0
    pattern = "COUNTIF"
    while i < len(sql):
        idx = sql.upper().find(pattern, i)
        if idx == -1:
            result.append(sql[i:])
            break
        result.append(sql[i:idx])
        paren_start = idx + len(pattern)
        while paren_start < len(sql) and sql[paren_start] in " \t\n":
            paren_start += 1
        if paren_start >= len(sql) or sql[paren_start] != "(":
            result.append(sql[idx:paren_start])
            i = paren_start
            continue

        depth = 0
        j = paren_start
        while j < len(sql):
            if sql[j] == "(":
                depth += 1
            elif sql[j] == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1

        condition = sql[paren_start + 1 : j].strip()
        pg_expr = f"COUNT(*) FILTER (WHERE {condition})"
        result.append(pg_expr)
        i = j + 1

    return "".join(result)
