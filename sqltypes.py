"""SQLite type constants.

This module provides constants for SQLite storage classes and common
type names / affinities. SQLite accepts arbitrary type names, but it
classifies them into affinities and uses the following storage classes:
NULL, INTEGER, REAL, TEXT, BLOB. See https://www.sqlite.org/datatype3.html
for details.
"""
# !! Largely Copilot-generated !!

# Storage classes
NULL = "NULL"
INTEGER = "INTEGER"
REAL = "REAL"
TEXT = "TEXT"
BLOB = "BLOB"

# Common type affinities / type names
NUMERIC = "NUMERIC"
BOOLEAN = "BOOLEAN"
DATE = "DATE"
DATETIME = "DATETIME"
DECIMAL = "DECIMAL"
FLOAT = "FLOAT"
DOUBLE = "DOUBLE"
INT = "INT"
BIGINT = "BIGINT"

_legal_types = {
    NULL,
    INTEGER,
    REAL,
    TEXT,
    BLOB,
    NUMERIC,
    BOOLEAN,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    DOUBLE,
    INT,
    BIGINT,
}

def typename_islegal(name: str) -> bool:
    return name in _legal_types
