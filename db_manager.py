import sqlite3 as sql
import os
import re
import hashlib
from typing import Optional

from computation_object_data import ComputationObjectData

# Some copilot help

COMPUTATION_OBJECT_RELATION_PREFIX = "co_"

class DBManager:

    conn: Optional[sql.Connection] = None

    @staticmethod
    def initialize(db_path: str):
        """Create (or open) a SQLite database at ``db_path`` and initialize
        a minimal schema used by the cache.

        - Creates parent directories if necessary (except for ``":memory:"``).
        - Enables foreign key support.
        - Creates a simple ``ccache_meta`` table to store metadata like version.

        Returns the open :class:`sqlite3.Connection`.
        """
        # Ensure the parent directory exists unless using in-memory DB
        if db_path not in (":memory", ":memory:"):
            parent = os.path.dirname(db_path)
            if parent:
                os.makedirs(parent, exist_ok=True)

        conn = sql.connect(db_path)
        # Prefer row factory for convenience when reading metadata
        conn.row_factory = sql.Row

        # Enable common pragmas
        conn.execute("PRAGMA foreign_keys = ON;")

        # create a table for keeping track of relations
        conn.execute("""
        CREATE TABLE IF NOT EXISTS relations (
            relation_name TEXT PRIMARY KEY,
            computation_object_name TEXT,
            metadata_str_rep TEXT,
            creation_date DATETIME DEFAULT (CURRENT_TIMESTAMP)
        )
        """)

        DBManager.conn = conn
    
    @staticmethod
    def _relation_table_name(object_identifier: str, object_data: ComputationObjectData) -> str:
        """Construct a safe table name for a computation object relation.

        If ``object_data`` is provided, append an 8-character (hex) deterministic
        hash computed from the sorted metadata name:type pairs to the table name.
        This ensures table names change when the metadata schema changes.
        """
        # sanitize identifier to safe table name: allow letters, digits and underscore
        safe = re.sub(r"\W+", "_", object_identifier)
        digest = hashlib.sha256(object_data.metadata.get_string_representation().encode("utf-8")).hexdigest()[:8]

        return f"{COMPUTATION_OBJECT_RELATION_PREFIX}{safe}_{digest}"

    @staticmethod
    def create_computation_object_relation(object_identifier: str, object_data: ComputationObjectData):
        if DBManager.conn is None:
            raise RuntimeError("DBManager.initialize must be called before creating relations")

        # Check most-recent relation for this computation object identifier
        cur = DBManager.conn.execute(
            "SELECT relation_name, metadata_str_rep FROM relations WHERE computation_object_name = ? ORDER BY creation_date DESC LIMIT 1",
            (object_identifier,)
        )
        row = cur.fetchone()

        new_meta_items = object_data.metadata.get_metadata_items()

        if row is not None:
            old_meta_str = row["metadata_str_rep"] if row["metadata_str_rep"] is not None else ""
            try:
                from computation_object_metadata import ComputationObjectMetadata
                old_meta = ComputationObjectMetadata.string_representation_to_metadata_dict(old_meta_str) if old_meta_str else {}
            except Exception:
                old_meta = {}

            old_keys = set(old_meta.keys())
            new_keys = set(new_meta_items.keys())

            # If new metadata is a (proper or equal) superset of the old one,
            # alter the existing table to add missing columns and update the
            # stored metadata representation.
            if new_keys.issuperset(old_keys):
                missing = sorted(new_keys - old_keys)
                if missing:
                    table_name = row["relation_name"]
                    for col in missing:
                        col_type = new_meta_items[col]
                        DBManager.conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {col_type};')

                    # update metadata_str_rep and bump creation_date to now
                    DBManager.conn.execute(
                        "UPDATE relations SET metadata_str_rep = ?, creation_date = CURRENT_TIMESTAMP WHERE relation_name = ?",
                        (object_data.metadata.get_string_representation(), table_name),
                    )
                    DBManager.conn.commit()
                return row["relation_name"]

        # Otherwise create a new relation table (schema changed incompatibly)
        table_name = DBManager._relation_table_name(object_identifier, object_data)

        cols = ["uid TEXT PRIMARY KEY"]
        for varname, sql_type in new_meta_items.items():
            cols.append(f'"{varname}" {sql_type}')

        sql_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'
        DBManager.conn.execute(sql_stmt)

        # Insert or replace relation entry with current metadata string
        DBManager.conn.execute(
            "INSERT OR REPLACE INTO relations (relation_name, computation_object_name, metadata_str_rep) VALUES (?, ?, ?);",
            (table_name, object_identifier, object_data.metadata.get_string_representation()),
        )

        DBManager.conn.commit()
        return table_name

    @staticmethod
    def insert_computation_object(obj: any, uid: str, object_data: ComputationObjectData):
        """Insert a computation object instance and its computed metadata.

        - Ensures the relation/table exists.
        - Computes metadata via the ComputationObjectData and inserts a row
          with the given `id` and metadata fields.
        """
        if DBManager.conn is None:
            raise RuntimeError("DBManager.initialize must be called before inserting objects")

        # ensure table exists (schema derived from metadata description)
        DBManager.create_computation_object_relation(object_data.object_identifier, object_data)

        table = DBManager._relation_table_name(object_data.object_identifier, object_data)

        # compute the metadata values
        metadata = object_data.metadata.compute_metadata(obj)
        # metadata is expected to be a dict varname->value
        cols = ["uid"] + list(metadata.keys())
        placeholders = ",".join(["?"] * len(cols))
        sql_stmt = f'INSERT INTO "{table}" ({",".join(cols)}) VALUES ({placeholders})'

        params = [uid] + [metadata[k] for k in metadata.keys()]
        DBManager.conn.execute(sql_stmt, params)
        DBManager.conn.commit()
