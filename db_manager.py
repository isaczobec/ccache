import sqlite3 as sql
import os
import re
import hashlib
from typing import Optional

from computation_object_data import ComputationObjectData
from computation_object_metadata import ComputationObjectMetadata


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

    # This method is pretty much entirely implemented by copilot, should check behaviour
    @staticmethod
    def get_or_create_computation_object_relation(object_identifier: str, object_data: ComputationObjectData) -> str:
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
                old_meta = ComputationObjectMetadata.string_representation_to_metadata_dict(old_meta_str) if old_meta_str else {}
            except Exception:
                old_meta = {}

            old_keys = set(old_meta.keys())
            print(old_keys)
            new_keys = set(new_meta_items.keys())
            print(new_keys)

            # If new metadata is a (proper or equal) superset of the old one,
            # we normally can ALTER the existing table to add missing columns.
            # However, if any overlapping column has a changed type, this is an
            # incompatible change and we need to create a new relation/table and
            # copy the data over (attempting CASTs where possible).
            if new_keys.issuperset(old_keys):
                # detect type changes for overlapping columns
                overlapping = old_keys & new_keys
                changed_types = sorted([k for k in overlapping if old_meta.get(k) != new_meta_items.get(k)])
                if changed_types:
                    # create a new table and copy rows using CAST where possible
                    table_name = DBManager._relation_table_name(object_identifier, object_data)
                    print("new table name: " + table_name)

                    cols = ["uid TEXT PRIMARY KEY"]
                    for varname, sql_type in new_meta_items.items():
                        cols.append(f'"{varname}" {sql_type}')

                    sql_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'
                    DBManager.conn.execute(sql_stmt)

                    old_table = row["relation_name"]

                    new_vars = list(new_meta_items.keys())
                    select_exprs = ['"uid"']
                    for var in new_vars:
                        if var in old_meta:
                            select_exprs.append(f'CAST("{var}" AS {new_meta_items[var]}) AS "{var}"')
                        else:
                            select_exprs.append(f'NULL AS "{var}"')

                    columns_list = ','.join(['"uid"'] + [f'"{v}"' for v in new_vars])
                    insert_stmt = f'INSERT INTO "{table_name}" ({columns_list}) SELECT {", ".join(select_exprs)} FROM "{old_table}";'

                    try:
                        DBManager.conn.execute(insert_stmt)
                    except Exception:
                        # Fallback row-by-row copy
                        cur2 = DBManager.conn.execute(f'SELECT * FROM "{old_table}";')
                        rows = cur2.fetchall()
                        placeholders = ','.join(['?'] * (1 + len(new_vars)))
                        insert_sql = f'INSERT INTO "{table_name}" ({columns_list}) VALUES ({placeholders});'

                        for r in rows:
                            params = [r['uid']]
                            for v in new_vars:
                                params.append(r[v] if v in r.keys() else None)
                            DBManager.conn.execute(insert_sql, params)

                    DBManager.conn.execute(
                        "INSERT OR REPLACE INTO relations (relation_name, computation_object_name, metadata_str_rep) VALUES (?, ?, ?);",
                        (table_name, object_identifier, object_data.metadata.get_string_representation()),
                    )

                    DBManager.conn.commit()
                    return table_name

                # no type changes detected; just add missing columns
                missing = sorted(new_keys - old_keys)
                if missing:

                    new_table_name = DBManager._relation_table_name(object_identifier, object_data)

                    old_table_name = row["relation_name"]

                    # If the new table name already exists in the DB, merge rows from
                    # the old table into the existing new table rather than renaming
                    # the old table. Only copy rows whose uid does not already exist
                    # in the destination table, then bump the creation_date on the
                    # existing relation entry.
                    cur_check = DBManager.conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
                        (new_table_name,)
                    )
                    exists = cur_check.fetchone() is not None

                    new_vars = list(new_meta_items.keys())
                    columns_list = ','.join(['"uid"'] + [f'"{v}"' for v in new_vars])

                    if exists:
                        select_exprs = ['"uid"'] + [f'"{v}"' if v in old_meta else f'NULL AS "{v}"' for v in new_vars]
                        insert_stmt = f'INSERT INTO "{new_table_name}" ({columns_list}) SELECT {", ".join(select_exprs)} FROM "{old_table_name}" WHERE uid NOT IN (SELECT uid FROM "{new_table_name}");'
                        try:
                            DBManager.conn.execute(insert_stmt)
                        except Exception:
                            # Fallback: python merge with UID de-duplication
                            cur_old = DBManager.conn.execute(f'SELECT * FROM "{old_table_name}";')
                            rows = cur_old.fetchall()
                            cur_new = DBManager.conn.execute(f'SELECT uid FROM "{new_table_name}";')
                            existing_uids = {r['uid'] for r in cur_new.fetchall()}
                            placeholders = ','.join(['?'] * (1 + len(new_vars)))
                            insert_sql = f'INSERT INTO "{new_table_name}" ({columns_list}) VALUES ({placeholders});'
                            for r in rows:
                                if r['uid'] in existing_uids:
                                    continue
                                params = [r['uid']] + [r[v] if v in r.keys() else None for v in new_vars]
                                DBManager.conn.execute(insert_sql, params)

                        # bump creation_date (and update metadata representation) on the existing relation
                        DBManager.conn.execute(
                            "UPDATE relations SET metadata_str_rep = ?, creation_date = CURRENT_TIMESTAMP WHERE relation_name = ?",
                            (object_data.metadata.get_string_representation(), new_table_name),
                        )
                        DBManager.conn.commit()
                        return new_table_name

                    # otherwise, add missing columns to old table and rename it once
                    for col in missing:
                        col_type = new_meta_items[col]
                        DBManager.conn.execute(f'ALTER TABLE "{old_table_name}" ADD COLUMN "{col}" {col_type};')

                    DBManager.conn.execute(f'ALTER TABLE "{old_table_name}" RENAME TO "{new_table_name}";')

                    # update metadata_str_rep and bump creation_date to now
                    DBManager.conn.execute(
                        "UPDATE relations SET relation_name = ?, metadata_str_rep = ?, creation_date = CURRENT_TIMESTAMP WHERE relation_name = ?",
                        (new_table_name, object_data.metadata.get_string_representation(), old_table_name),
                    )
                    DBManager.conn.commit()
                    return new_table_name

        # Otherwise create a new relation table (schema changed incompatibly)
        table_name = DBManager._relation_table_name(object_identifier, object_data)

        cols = ["uid TEXT PRIMARY KEY"]
        for varname, sql_type in new_meta_items.items():
            cols.append(f'"{varname}" {sql_type}')

        sql_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'
        DBManager.conn.execute(sql_stmt)

        # If we have an existing relation, try to copy its contents into the
        # newly created table. This handles deletion of columns in the new
        # metadata (we keep the old relation untouched) and attempts to cast
        # values to the new column types where possible. If the SQL-based
        # cast fails, fall back to a row-by-row copy.
        if row is not None:
            old_table = row["relation_name"]

            new_vars = list(new_meta_items.keys())
            # Build SELECT expressions using CAST for existing columns, NULL
            # for newly added columns.
            select_exprs = ['"uid"']
            for var in new_vars:
                if var in old_meta:
                    # Use CAST to convert values to the new column type where possible
                    select_exprs.append(f'CAST("{var}" AS {new_meta_items[var]}) AS "{var}"')
                else:
                    select_exprs.append(f'NULL AS "{var}"')

            columns_list = ','.join(['"uid"'] + [f'"{v}"' for v in new_vars])
            # Only insert rows that don't already exist in the destination table
            insert_stmt = f'INSERT INTO "{table_name}" ({columns_list}) SELECT {", ".join(select_exprs)} FROM "{old_table}" WHERE uid NOT IN (SELECT uid FROM "{table_name}");'

            try:
                DBManager.conn.execute(insert_stmt)
            except Exception:
                # Fallback: copy rows in Python without explicit casting, with UID de-duplication.
                cur2 = DBManager.conn.execute(f'SELECT * FROM "{old_table}";')
                rows = cur2.fetchall()
                cur_dest = DBManager.conn.execute(f'SELECT uid FROM "{table_name}";')
                dest_uids = {r['uid'] for r in cur_dest.fetchall()}
                placeholders = ','.join(['?'] * (1 + len(new_vars)))
                insert_sql = f'INSERT INTO "{table_name}" ({columns_list}) VALUES ({placeholders});'

                for r in rows:
                    if r['uid'] in dest_uids:
                        continue
                    params = [r['uid']]
                    for v in new_vars:
                        # Use the value from the old row if present, else None
                        params.append(r[v] if v in r.keys() else None)
                    DBManager.conn.execute(insert_sql, params)

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
        DBManager.get_or_create_computation_object_relation(object_data.object_identifier, object_data)

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
