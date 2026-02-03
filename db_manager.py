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
        # conn.execute("""
        # CREATE TABLE IF NOT EXISTS metadata_relations (

        # )
        # """)

        DBManager.conn = conn
    
    @staticmethod
    def _relation_table_name(object_identifier: str, object_data: ComputationObjectData | None = None) -> str:
        """Construct a safe table name for a computation object relation.

        If ``object_data`` is provided, append an 8-character (hex) deterministic
        hash computed from the sorted metadata name:type pairs to the table name.
        This ensures table names change when the metadata schema changes.
        """
        # sanitize identifier to safe table name: allow letters, digits and underscore
        safe = re.sub(r"\W+", "_", object_identifier)

        if object_data is None:
            return f"{COMPUTATION_OBJECT_RELATION_PREFIX}{safe}"

        meta_items = object_data.metadata.get_metadata_items()
        if not meta_items:
            return f"{COMPUTATION_OBJECT_RELATION_PREFIX}{safe}"

        # build a deterministic representation of metadata items
        parts = [f"{k}:{v}" for k, v in sorted(meta_items.items())]
        joined = ";".join(parts)
        digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:8]

        return f"{COMPUTATION_OBJECT_RELATION_PREFIX}{safe}_{digest}"

    @staticmethod
    def create_computation_object_relation(object_identifier: str, object_data: ComputationObjectData):
        if DBManager.conn is None:
            raise RuntimeError("DBManager.initialize must be called before creating relations")

        table = DBManager._relation_table_name(object_identifier, object_data)

        cols = ["uid TEXT PRIMARY KEY"]
        # object_data.metadata.get_metadata_items expected to return dict[varname, sqltype]
        for varname, sql_type in object_data.metadata.get_metadata_items().items():
            cols.append(f"{varname} {sql_type}")

        sql_stmt = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(cols)});'
        DBManager.conn.execute(sql_stmt)
        DBManager.conn.commit()

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
