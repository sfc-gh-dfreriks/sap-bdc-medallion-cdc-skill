"""Connection helper.

Works in two contexts without a code change:

  outside Snowflake  a laptop, using a named entry in ~/.snowflake/connections.toml
  inside Snowflake   a Notebook, Streamlit or stored procedure, using the active
                     Snowpark session

Nothing in this package writes to a share. The assessment is read-only; the two
optional probes that create objects do so in a scratch database and drop it.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pandas as pd


def in_snowflake() -> bool:
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        return True
    except Exception:
        return False


class Session:
    """Uniform query interface over a Snowpark session or the Python connector."""

    def __init__(self, connection: str | None = None):
        self.mode = "snowpark" if in_snowflake() else "connector"
        if self.mode == "snowpark":
            from snowflake.snowpark.context import get_active_session
            self._s = get_active_session()
        else:
            self._c = _connect(connection)

    def df(self, sql: str) -> pd.DataFrame:
        if self.mode == "snowpark":
            return _floats(self._s.sql(sql).to_pandas())
        cur = self._c.cursor()
        try:
            cur.execute(sql)
            try:
                return _floats(cur.fetch_pandas_all())
            except Exception:
                # SHOW and DESCRIBE do not return Arrow results, so
                # fetch_pandas_all is unavailable for them.
                rows = cur.fetchall()
                return _floats(pd.DataFrame(rows, columns=[d[0] for d in cur.description]))
        finally:
            cur.close()

    def rows(self, sql: str) -> list[dict]:
        return self.df(sql).to_dict("records")

    def one(self, sql: str) -> dict:
        r = self.rows(sql)
        return r[0] if r else {}

    def execute(self, sql: str) -> None:
        if self.mode == "snowpark":
            self._s.sql(sql).collect()
            return
        cur = self._c.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def try_execute(self, sql: str) -> tuple[bool, str | None]:
        """Run a statement and return whether it succeeded plus the error text.

        Used by the capability probe, where a refusal is the finding rather than
        a failure to be handled.
        """
        try:
            self.execute(sql)
            return True, None
        except Exception as e:
            return False, " ".join(str(e).split())

    def last_query_bytes(self) -> dict:
        """Scan statistics for the statement just run, read from query history.

        Query history needs a database context, so callers must have set one.
        """
        qid = self.one("SELECT LAST_QUERY_ID() AS Q").get("Q")
        if not qid:
            return {}
        r = self.rows(
            "SELECT BYTES_SCANNED, ROWS_PRODUCED, TOTAL_ELAPSED_TIME "
            "FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) "
            f"WHERE QUERY_ID = '{qid}'")
        return r[0] if r else {}


def _connect(name: str | None):
    import snowflake.connector
    try:
        return snowflake.connector.connect(connection_name=name) if name \
            else snowflake.connector.connect()
    except TypeError:
        # older connector: read connections.toml and load the key ourselves
        import tomllib
        cfg = tomllib.loads((Path.home() / ".snowflake/connections.toml").read_text())
        entry = cfg[name] if name else next(iter(cfg.values()))
        kw = {k: entry[k] for k in
              ("account", "user", "role", "warehouse", "database", "schema") if k in entry}
        if "private_key_path" in entry:
            from cryptography.hazmat.primitives import serialization
            kw["private_key"] = serialization.load_pem_private_key(
                Path(entry["private_key_path"]).read_bytes(),
                password=(entry.get("private_key_passphrase") or "").encode() or None,
            ).private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption())
        else:
            for k in ("password", "authenticator"):
                if k in entry:
                    kw[k] = entry[k]
        return snowflake.connector.connect(**kw)


def _floats(df: pd.DataFrame) -> pd.DataFrame:
    """Snowflake NUMBER arrives as decimal.Decimal, which will not combine with
    numpy floats. Coerce once here rather than at every call site."""
    for c in df.columns:
        if df[c].dtype == object:
            nn = df[c].dropna()
            if len(nn) and isinstance(nn.iloc[0], Decimal):
                df[c] = df[c].astype("float64")
    return df


def q(identifier: str) -> str:
    """Quote an identifier only when it needs it.

    SAP BDC schema names contain a colon (dp_finance_v2_srv:v1) and table names are
    mixed case, so unquoted references fail.
    """
    if identifier.isupper() and identifier.replace("_", "").replace("$", "").isalnum():
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def fqn(*parts: str) -> str:
    return ".".join(q(p) for p in parts)
