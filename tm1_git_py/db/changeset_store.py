from pathlib import Path
import os
import threading
from typing import Any, Iterable, Iterator, Optional

from tm1_git_py.db._worker_db import WorkerDBLease, WorkerDBRegistry


_QUERY_COLUMNS = (
    "seq",
    "change_type",
    "object_type",
    "uri",
    "apply",
    "body_json",
    "dim_name",
    "hier_name",
    "object_name",
    "cube_name",
    "process_name",
    "chore_name",
)


class ChangesetStore:
    _instances: dict[str, "ChangesetStore"] = {}
    _instances_lock = threading.RLock()

    def __init__(self, *, changeset_id: str, base_dir: Optional[str] = None):
        self.db_path = self.path_for(changeset_id=changeset_id, base_dir=base_dir)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db_lease: WorkerDBLease = WorkerDBRegistry.acquire(
            str(self.db_path),
            execute_init=(
                "PRAGMA foreign_keys=OFF",
                "PRAGMA journal_mode=WAL",
                "PRAGMA synchronous=NORMAL",
                "PRAGMA temp_store=MEMORY",
                "PRAGMA busy_timeout=30000",
            ),
        )
        self._db = self._db_lease.worker
        self._closed = False
        self._session_refs = 0
        self._lifecycle_lock = threading.RLock()
        self._initialize()

    @classmethod
    def path_for(cls, *, changeset_id: str, base_dir: Optional[str] = None) -> Path:
        root = (
            Path(base_dir).expanduser().resolve()
            if base_dir
            else (Path.home().resolve() / ".tm1gitpy" / ".cache")
        )
        return root / f"changeset-{changeset_id}.sqlite"


    @classmethod
    def for_changeset_id(
        cls,
        *,
        changeset_id: str,
        base_dir: Optional[str] = None
    ) -> "ChangesetStore":
        db_path = cls.path_for(changeset_id=changeset_id, base_dir=base_dir)
        key = os.path.abspath(db_path)
        with cls._instances_lock:
            existing = cls._instances.get(key)
            if existing is not None:
                if not existing._closed:
                    return existing
                cls._instances.pop(key, None)
            created = cls(
                changeset_id=changeset_id,
                base_dir=base_dir
            )
            cls._instances[key] = created
            return created

    def close(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            self._db_lease.release()

    @classmethod
    def close_all(cls) -> None:
        with cls._instances_lock:
            stores = list(cls._instances.values())
            cls._instances.clear()
        for store in stores:
            store.close()
            WorkerDBRegistry.force_close(str(store.db_path))

    def _table_columns(self, table_name: str) -> list[str]:
        rows = self._db.fetch_all(
            "SELECT name FROM pragma_table_info(?)",
            (table_name,),
        )
        return [str(row[0]) for row in rows]

    def _table_exists(self, table_name: str) -> bool:
        row = self._db.fetch_one(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        )
        return row is not None

    def _schema_needs_reset(self) -> bool:
        if not self._table_exists("changes"):
            return False
        expected = {
            "seq",
            "change_type",
            "object_type",
            "uri",
            "apply",
            "body_json",
            "dim_name",
            "hier_name",
            "object_name",
            "cube_name",
            "process_name",
            "chore_name",
            "type_rank",
            "precedence_rank",
            "body_name",
        }
        return set(self._table_columns("changes")) != expected

    def _initialize(self) -> None:
        if self._schema_needs_reset():
            self._db.run_sync("DROP TABLE IF EXISTS changes")
        self._db.run_sync(
            """
            CREATE TABLE IF NOT EXISTS changes (
                seq INTEGER PRIMARY KEY,
                change_type TEXT NOT NULL,
                object_type TEXT NOT NULL,
                uri TEXT NOT NULL,
                apply INTEGER NOT NULL,
                body_json TEXT NOT NULL,
                dim_name TEXT NULL,
                hier_name TEXT NULL,
                object_name TEXT NULL,
                cube_name TEXT NULL,
                process_name TEXT NULL,
                chore_name TEXT NULL,
                type_rank INTEGER NOT NULL,
                precedence_rank INTEGER NOT NULL,
                body_name TEXT NOT NULL
            )
            """
        )
        self._db.run_sync(
            """
            CREATE INDEX IF NOT EXISTS idx_changes_apply_sort
            ON changes(apply, type_rank, precedence_rank, body_name, uri, seq)
            """
        )
        self._db.run_sync(
            """
            CREATE INDEX IF NOT EXISTS idx_changes_sort
            ON changes(type_rank, precedence_rank, body_name, uri, seq)
            """
        )
        self._db.run_sync(
            """
            CREATE INDEX IF NOT EXISTS idx_changes_dim_hier_object_seq
            ON changes(dim_name, hier_name, object_name, seq)
            """
        )
        self._db.run_sync(
            """
            CREATE INDEX IF NOT EXISTS idx_changes_cube_object_seq
            ON changes(cube_name, object_name, seq)
            """
        )
        # self._db.commit()

    @staticmethod
    def _change_row_to_tuple(row: dict[str, Any], seq: int) -> tuple[Any, ...]:
        """Build INSERT tuple for one change row; ``seq`` is the assigned primary key."""
        return (
            int(seq),
            str(row["change_type"]),
            str(row["object_type"]),
            str(row["uri"]),
            1 if bool(row["apply"]) else 0,
            str(row["body_json"]),
            row.get("dim_name"),
            row.get("hier_name"),
            row.get("object_name"),
            row.get("cube_name"),
            row.get("process_name"),
            row.get("chore_name"),
            int(row["type_rank"]),
            int(row["precedence_rank"]),
            str(row["body_name"]),
        )

    def _prepare_rows(self, rows: Iterable[dict[str, Any]]) -> list[tuple[Any, ...]]:
        return [self._change_row_to_tuple(row, int(row["seq"])) for row in rows]

    def replace_rows(self, rows: Iterable[dict[str, Any]]) -> None:
        self._db.run_sync("DELETE FROM changes")
        prepared_rows = self._prepare_rows(rows)
        self._db.executemany_and_fetch(
            """
            INSERT INTO changes(
                seq, change_type, object_type, uri, apply, body_json,
                dim_name, hier_name, object_name, cube_name, process_name, chore_name,
                type_rank, precedence_rank, body_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            prepared_rows,
        )

    def append_rows(self, rows: Iterable[dict[str, Any]]) -> None:
        current_seq = self.count_rows()
        prepared_rows: list[tuple[Any, ...]] = []
        for offset, row in enumerate(rows):
            prepared_rows.append(self._change_row_to_tuple(row, current_seq + offset))
        self._db.executemany_and_fetch(
            """
            INSERT INTO changes(
                seq, change_type, object_type, uri, apply, body_json,
                dim_name, hier_name, object_name, cube_name, process_name, chore_name,
                type_rank, precedence_rank, body_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            prepared_rows,
        )
        self._db.commit()

    def count_rows(self) -> int:
        row = self._db.fetch_one("SELECT COUNT(*) FROM changes")
        return int(row[0]) if row is not None else 0

    def clear(self) -> None:
        self._db.run_sync("DELETE FROM changes")

    @staticmethod
    def _order_clause(sorted_order: bool) -> str:
        if not sorted_order:
            return "seq"
        return "type_rank, precedence_rank, body_name, uri, seq"

    @staticmethod
    def _row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
        return dict(zip(_QUERY_COLUMNS, row))

    def iter_rows(
        self,
        *,
        apply_only: Optional[bool] = None,
        offset: int = 0,
        limit: Optional[int] = None,
        sorted_order: bool = True,
    ) -> Iterator[dict[str, Any]]:
        where_clauses: list[str] = []
        params: list[Any] = []
        if apply_only is not None:
            where_clauses.append("apply=?")
            params.append(1 if apply_only else 0)

        sql = (
            "SELECT seq, change_type, object_type, uri, apply, body_json, "
            "dim_name, hier_name, object_name, cube_name, process_name, chore_name "
            "FROM changes"
        )
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += f" ORDER BY {self._order_clause(sorted_order)}"

        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend([int(limit), max(0, int(offset))])
        elif offset > 0:
            sql += " LIMIT -1 OFFSET ?"
            params.append(max(0, int(offset)))

        for row in self._db.fetch_all(sql, tuple(params)):
            yield self._row_to_dict(row)

    def row_at(self, index: int, *, sorted_order: bool = True) -> Optional[dict[str, Any]]:
        if index < 0:
            return None
        sql = (
            "SELECT seq, change_type, object_type, uri, apply, body_json, "
            "dim_name, hier_name, object_name, cube_name, process_name, chore_name "
            "FROM changes "
            f"ORDER BY {self._order_clause(sorted_order)} LIMIT 1 OFFSET ?"
        )
        row = self._db.fetch_one(sql, (index,))
        return self._row_to_dict(row) if row is not None else None

    @staticmethod
    def _is_uri_selected(uri: str, rules_json: str) -> int:
        from tm1_git_py.services.filter import FilterRules
        import orjson

        try:
            parsed = orjson.loads(rules_json) if rules_json else []
        except Exception:
            parsed = []
        if not isinstance(parsed, list):
            parsed = []
        rules = FilterRules([str(rule) for rule in parsed])
        return 0 if rules.should_exclude(uri) else 1

    @staticmethod
    def _desired_apply(uri: str, rules_json: str, current_apply: int) -> int:
        """Return next apply state for changeset-filter.

        Semantics:
        - ignored by effective rules => apply = 0
        - unignored/force-included by effective rules => apply = 1
        - not matched by aggregated filter rules => preserve prior apply
        """
        from tm1_git_py.services.filter import FilterRules, _parse_object_selector
        import orjson

        try:
            parsed = orjson.loads(rules_json) if rules_json else []
        except Exception:
            parsed = []
        if not isinstance(parsed, list):
            parsed = []
        rules = FilterRules([str(rule) for rule in parsed])
        current = 1 if int(current_apply) else 0

        if not rules.has_rules:
            return current

        if rules.should_exclude(uri):
            return 0

        context = _parse_object_selector(uri)
        if context and rules._is_force_include_related_to_target(context):
            return 1

        return current

    def query_rows(
        self,
        *,
        filter_rules: Optional[list[str]] = None,
        offset: int = 0,
        limit: Optional[int] = None,
        apply_only: Optional[bool] = None,
        sorted_order: bool = True,
    ) -> Iterator[dict[str, Any]]:
        where_clauses: list[str] = []
        params: list[Any] = []
        if apply_only is not None:
            where_clauses.append("apply=?")
            params.append(1 if apply_only else 0)

        sql = (
            "SELECT seq, change_type, object_type, uri, apply, body_json, "
            "dim_name, hier_name, object_name, cube_name, process_name, chore_name "
            "FROM changes"
        )
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += f" ORDER BY {self._order_clause(sorted_order)}"

        # Filter-rule predicates use Python helpers, so we apply offset/limit
        # in Python after the SQL projection rather than via SQL when the
        # filter is active.
        if filter_rules:
            import orjson

            rules_json = orjson.dumps([str(rule) for rule in filter_rules]).decode("utf-8")
            rows = self._db.fetch_all(sql, tuple(params))
            selected: list[tuple[Any, ...]] = [
                row for row in rows if self._is_uri_selected(str(row[3]), rules_json) == 1
            ]
            start = max(0, int(offset))
            end = (start + max(0, int(limit))) if limit is not None else len(selected)
            for row in selected[start:end]:
                yield self._row_to_dict(row)
            return

        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend([int(limit), max(0, int(offset))])
        elif offset > 0:
            sql += " LIMIT -1 OFFSET ?"
            params.append(max(0, int(offset)))

        for row in self._db.fetch_all(sql, tuple(params)):
            yield self._row_to_dict(row)

    def toggle_apply(self, *, filter_rules: Optional[list[str]] = None) -> int:
        if not filter_rules:
            # No filters means no state transition: preserve existing apply flags.
            return 0

        import orjson

        rules_json = orjson.dumps([str(rule) for rule in filter_rules]).decode("utf-8")
        rows = self._db.fetch_all("SELECT seq, uri, apply FROM changes")
        updates: list[tuple[int, int]] = []
        for seq, uri, current_apply in rows:
            desired = self._desired_apply(str(uri), rules_json, int(current_apply))
            if int(current_apply) != desired:
                updates.append((desired, int(seq)))
        if not updates:
            return 0
        self._db.executemany_and_fetch("UPDATE changes SET apply = ? WHERE seq = ?", updates)
        return len(updates)

    def summary_counts(self) -> dict[str, int]:
        rows = self._db.fetch_all(
            "SELECT change_type, COUNT(*) AS cnt FROM changes GROUP BY change_type"
        )
        summary = {"add": 0, "remove": 0, "modify": 0}
        for row in rows:
            change_type = str(row[0]).lower()
            summary[change_type] = int(row[1])
        return summary
