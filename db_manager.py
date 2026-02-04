import sqlite3 as sql
import os
import re
import hashlib
from typing import Optional
import uuid

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
            relation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            relation_name TEXT,
            timestamp DATETIME DEFAULT (CURRENT_TIMESTAMP),
            co_identifier TEXT,
            metadata_rep TEXT,
            metadata_hash TEXT
        )
        """)

        # create a table for data about the computation objects
        conn.execute("""
        CREATE TABLE IF NOT EXISTS computation_objects (
            uid TEXT PRIMARY KEY,
            co_identifier TEXT,
            timestamp DATETIME DEFAULT (CURRENT_TIMESTAMP),
            orig_metadata_hash TEXT
        )
        """)

        DBManager.conn = conn
    
    @staticmethod
    def _get_metadata_hash(metadata: ComputationObjectMetadata):
        return hashlib.sha256(metadata.get_string_representation().encode("utf-8")).hexdigest()[:8]

    @staticmethod
    def _create_relation_name(object_data: ComputationObjectData) -> str:
        """non deterministic"""
        return f"{object_data.object_identifier.replace(" ","_")}_{str(uuid.uuid4())[:8]}"

    @staticmethod
    def _reconcile_relation(relation_id: int, object_data: ComputationObjectData) -> str:
        # get the info of the relation
        cur = DBManager.conn.execute(
            "SELECT * FROM relations WHERE relation_id = ? ORDER BY timestamp DESC LIMIT 1",
            (relation_id,)
        )
        rel = cur.fetchone()

        new_metadata_hash = DBManager._get_metadata_hash(object_data.metadata)
        old_metadata_hash = rel["metadata_hash"]

        # if the metadata signatures are the same, simply return the old relation name and return
        if (new_metadata_hash == old_metadata_hash):
            return rel["relation_name"]
        
        # if they differ, create a new relation, and try to copy over the existing values
        new_relation_name = DBManager._create_co_relation(object_data)

        # get the overlap between old and new fields, copy over those
        old_meta_vars = ComputationObjectMetadata.string_representation_to_metadata_dict(rel["metadata_rep"])
        new_meta_vars = object_data.metadata.get_metadata_items()
        overlap_vars = list(set(old_meta_vars.keys()) & set(new_meta_vars.keys()))

        select_exprs = ["uid"] + [f"CAST({var} AS {new_meta_vars[var]}) AS {var}" for var in overlap_vars]
        overlap_vars = ["uid"] + overlap_vars # add uid

        # query the overlapping values
        copy_stmt = f"""
        INSERT INTO {new_relation_name} ({", ".join(str(v) for v in overlap_vars)})
        SELECT {", ".join(select_exprs)}
        FROM {rel["relation_name"]}
        ;
        """
        print(copy_stmt)
        DBManager.conn.execute(copy_stmt)
        DBManager.conn.commit()

        return new_relation_name

    @staticmethod
    def _create_co_relation(object_data: ComputationObjectData) -> str:
        new_relation_name = DBManager._create_relation_name(object_data) # new table including a uuid
        new_metadata_hash = DBManager._get_metadata_hash(object_data.metadata)
        new_relation_stmt = f"""
        CREATE TABLE {new_relation_name} (
        uid TEXT PRIMARY KEY,
        {", \n".join([f"{var} {type}" for var, type in object_data.metadata.get_metadata_items().items()])}
        );
        """

        insert_into_relation_table_stmt = f"""
        INSERT INTO relations(
            relation_name, 
            co_identifier, 
            metadata_rep, 
            metadata_hash
        ) 
        VALUES (
            ?, ?, ?, ?
        )
        """

        DBManager.conn.execute(new_relation_stmt)
        DBManager.conn.execute(insert_into_relation_table_stmt, (
            new_relation_name, 
            object_data.object_identifier, 
            object_data.metadata.get_string_representation(),
            new_metadata_hash
            ))
        DBManager.conn.commit()
        return new_relation_name


    @staticmethod
    def _get_co_relation(object_data: ComputationObjectData) -> str:
        if DBManager.conn is None:
            raise RuntimeError("DBManager.initialize must be called before creating relations")

        # Check most-recent relation for this computation object identifier
        cur = DBManager.conn.execute(
            "SELECT relation_id, relation_name, metadata_rep FROM relations WHERE co_identifier = ? ORDER BY timestamp DESC LIMIT 1",
            (object_data.object_identifier,)
        )
        row = cur.fetchone()

        relation_name = None

        if row is not None:
            relation_name = DBManager._reconcile_relation(row["relation_id"], object_data)
        else:
            relation_name = DBManager._create_co_relation(object_data)

        return relation_name
            



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
        relation_name = DBManager._get_co_relation(object_data)

        # compute the metadata values
        metadata = object_data.metadata.compute_metadata(obj)
        # metadata is expected to be a dict varname->value
        cols = ["uid"] + list(metadata.keys())
        placeholders = ",".join(["?"] * len(cols))
        stmt = f'INSERT INTO "{relation_name}" ({",".join(cols)}) VALUES ({placeholders})'

        params = [uid] + [metadata[k] for k in metadata.keys()]
        DBManager.conn.execute(stmt, params)

        # insert into the table tracking computation objects
        co_stmt = f"""
        INSERT INTO computation_objects(uid, co_identifier, orig_metadata_hash)
        VALUES (?, ?, ?)
        """
        DBManager.conn.execute(co_stmt, (uid, object_data.object_identifier, DBManager._get_metadata_hash(object_data.metadata)))

        DBManager.conn.commit()

    @staticmethod 
    def _resolve_query(query: str, remove_semicolons: bool = False):
        
        resolved_query = query
        matches = re.findall(r":[A-Za-z0-9]+", query)
        for m in matches:
            print(m)

            # see if the match is a relation name
            identifier = m[1:]
            stmt = f"""
                SELECT relation_name FROM relations
                WHERE co_identifier = ?
                ORDER BY timestamp DESC
                LIMIT 1
                ; 
            """
            cur = DBManager.conn.execute(stmt, (identifier, ))
            res = cur.fetchone()
            print(res)
            if res is not None:
                rel_name = res["relation_name"]
                print(rel_name)
                resolved_query = resolved_query.replace(m, rel_name)

        if remove_semicolons:
            resolved_query = resolved_query.replace(";","")
    
        return resolved_query

    @staticmethod
    def query(query: str):
        """
        performs a query.
        Words prefixed by ":" are replaced.
        """
        res_query = DBManager._resolve_query(query)
        print(res_query)
        cur = DBManager.conn.execute(res_query)
        DBManager.conn.commit()
        res = cur.fetchall()

        if res is None: return []
        return res
    
    @staticmethod
    def get_uids_and_co_ids(query: str): # Chatgpt generated
        """
        Returns a list of (uid, co_identifier) tuples.
        """
        if DBManager.conn is None:
            raise RuntimeError("DBManager.initialize must be called first")

        resolved_query = DBManager._resolve_query(query, remove_semicolons=True)

        stmt = f"""
        SELECT q.uid, co.co_identifier
        FROM (
            {resolved_query}
        ) AS q
        JOIN computation_objects AS co
        ON q.uid = co.uid
        ;
        """

        cur = DBManager.conn.execute(stmt)
        rows = cur.fetchall()

        return [(r["uid"], r["co_identifier"]) for r in rows]


    @staticmethod
    def print_most_recent_rows(object_data: ComputationObjectData):
        if DBManager.conn is None:
            raise RuntimeError("DBManager.initialize must be called first")

        # find most recent relation for this computation object
        cur = DBManager.conn.execute(
            """
            SELECT relation_name
            FROM relations
            WHERE co_identifier = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (object_data.object_identifier,)
        )
        row = cur.fetchone()

        if row is None:
            print("No relation found for computation object")
            return

        relation_name = row["relation_name"]

        # fetch all rows
        cur = DBManager.conn.execute(f'SELECT * FROM "{relation_name}"')
        rows = cur.fetchall()

        if not rows:
            print(f"Table '{relation_name}' is empty")
            return

        # print header
        columns = rows[0].keys()
        print(f"Table: {relation_name}")
        print(" | ".join(columns))
        print("-" * (len(columns) * 12))

        # print rows
        for r in rows:
            print(" | ".join(str(r[c]) for c in columns))

