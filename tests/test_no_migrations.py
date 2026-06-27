"""Schema policy guards: the schema files are the single source of truth and
there are NO DB migrations.

There must be exactly one DDL file per dialect per subsystem - four files:
core app (schema_sqlite.sql, schema_postgres.sql) and flow
(flow_events_sqlite.sql, flow_events_postgres.sql) - and no ALTER-based schema
evolution anywhere in the Python sources. A flow/app DB that predates a schema
change is recreated, never migrated.
"""
import glob
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_DIR = os.path.join(ROOT, "config", "schema")


def test_exactly_four_schema_files():
    files = sorted(os.path.basename(p)
                   for p in glob.glob(os.path.join(SCHEMA_DIR, "*.sql")))
    assert files == [
        "flow_events_postgres.sql",
        "flow_events_sqlite.sql",
        "schema_postgres.sql",
        "schema_sqlite.sql",
    ], f"expected exactly the 4 canonical schema files, found: {files}"


def test_no_alter_table_migrations_in_python():
    """No source applies ALTER TABLE / ADD COLUMN - that would be a migration."""
    pat = re.compile(r"alter\s+table|add\s+column", re.IGNORECASE)
    # core/version.py is a changelog/version string module - its release notes
    # describe schema changes in prose and are not executable DDL.
    skip = {os.path.join("core", "version.py")}
    offenders = []
    for p in glob.glob(os.path.join(ROOT, "core", "**", "*.py"), recursive=True) \
            + glob.glob(os.path.join(ROOT, "routes", "*.py")) \
            + glob.glob(os.path.join(ROOT, "web", "*.py")):
        if "__pycache__" in p or os.path.relpath(p, ROOT) in skip:
            continue
        with open(p, "r", encoding="utf-8") as fh:
            if pat.search(fh.read()):
                offenders.append(os.path.relpath(p, ROOT))
    assert not offenders, f"ALTER/ADD COLUMN (migration) found in: {offenders}"


def test_schema_files_create_only_no_alter():
    """The DDL files themselves create; they never ALTER."""
    pat = re.compile(r"alter\s+table|add\s+column", re.IGNORECASE)
    for p in glob.glob(os.path.join(SCHEMA_DIR, "*.sql")):
        with open(p, "r", encoding="utf-8") as fh:
            assert not pat.search(fh.read()), f"ALTER found in {p}"
