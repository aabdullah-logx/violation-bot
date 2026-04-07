"""
Local SQLite database – mirrors the QuickBase tables used by access_sc.py.
Tables are only created / checked when settings.LOCAL_DB is True.

Table mapping (QuickBase DBID → local table):
  bt935dtsk  →  health_metrics   (insert_into_quickbase_x)
  btyyfj2fy  →  az_claims        (insert_into_quickbase / update_az_claims)

Violation + Health data that was previously going to the remote MySQL via db.py:
  account_health   →  account_health
  listing_issues_sc →  listing_issues_sc
"""

import os
import sqlite3
import math
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_data.db")


# ── Connection helper ──────────────────────────────────────────────────────────
def get_connection():
    """Return a connection to the local SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


# ── Table creation ─────────────────────────────────────────────────────────────
def init_db():
    """
    Create all tables if they do not already exist.
    Call this once at startup when LOCAL_DB is True.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # -- health_metrics  (mirrors QuickBase table bt935dtsk) --
    # Field mapping from insert_into_quickbase_x:
    #   6 → date, 7 → store_name, 8 → health_status, 9 → health_rating,
    #  10 → odr, 11 → vtr, 12 → buybox, 13 → balance,
    #  14 → negative_feedback, 15 → a_to_z_claims, 16 → chargeback_claims,
    #  17 → late_shipment_rate, 18 → pre_fulfilment_cancel_rate


    # -- az_claims  (mirrors QuickBase table btyyfj2fy) --
    # The field numbers used in insert_into_quickbase / update_az_claims
    # are dynamic (dict keys); we store them as generic numbered columns.
    # Fields commonly seen: 3 (record_id), 9, 10 (order, asin → composite key '15')


    # -- account_health  (mirrors remote MySQL table via db.py) --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS account_health (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            storename                   TEXT,
            status                      TEXT,
            health_rating               TEXT,
            odr                         TEXT,
            vtr                         TEXT,
            buybox                      TEXT,
            balance                     TEXT,
            negative_feedback           TEXT,
            a_to_z_claims               TEXT,
            chargeback_claims           TEXT,
            late_shipment_rate          TEXT,
            pre_fulfilment_cancel_rate  TEXT,
            created_at                  TEXT DEFAULT (datetime('now'))
        )
    """)

    # -- listing_issues_sc  (mirrors remote MySQL table via db.py) --
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS listing_issues_sc (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            storename       TEXT,
            asin            TEXT,
            impact          TEXT,
            action_taken    TEXT,
            reason          TEXT,
            publish_time    TEXT,
            category        TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(storename, asin, publish_time, category)
        )
    """)

    conn.commit()
    conn.close()
    print(f"[LOCAL DB] Initialized – {DB_PATH}")


# ── Insert helpers (mirror QuickBase functions in access_sc.py) ────────────────

def insert_health_metrics(data_list):
    """
    Local replacement for insert_into_quickbase_x().
    Expects data_list in the same format:
      [{'6': {'value': …}, '7': {'value': …}, … '18': {'value': …}}]
    """
    conn = get_connection()
    cursor = conn.cursor()

    for record in data_list:
        cursor.execute("""
            INSERT INTO health_metrics
                (date, store_name, health_status, health_rating,
                 odr, vtr, buybox, balance,
                 negative_feedback, a_to_z_claims, chargeback_claims,
                 late_shipment_rate, pre_fulfilment_cancel_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.get('6', {}).get('value', ''),
            record.get('7', {}).get('value', ''),
            record.get('8', {}).get('value', ''),
            record.get('9', {}).get('value', ''),
            str(record.get('10', {}).get('value', '')),
            str(record.get('11', {}).get('value', '')),
            str(record.get('12', {}).get('value', '')),
            record.get('13', {}).get('value', 0),
            str(record.get('14', {}).get('value', '')),
            str(record.get('15', {}).get('value', '')),
            str(record.get('16', {}).get('value', '')),
            str(record.get('17', {}).get('value', '')),
            str(record.get('18', {}).get('value', '')),
        ))

    conn.commit()
    conn.close()
    print(f"[LOCAL DB] Inserted {len(data_list)} health_metrics record(s)")


def insert_az_claims(data_list):
    """
    Local replacement for insert_into_quickbase().
    Expects data_list as a list of dicts with numeric keys → values.
    """
    conn = get_connection()
    cursor = conn.cursor()

    def safe(val):
        if val is None:
            return None
        if isinstance(val, float) and math.isnan(val):
            return None
        return str(val)

    for record in data_list:
        # Build a dict of field_N → value
        fields = {}
        for k, v in record.items():
            col = f"field_{k}"
            fields[col] = safe(v)

        # Only insert columns that exist in the table (field_3 .. field_20)
        valid_cols = [f"field_{i}" for i in range(3, 21)]
        cols_to_insert = [c for c in valid_cols if c in fields]
        vals_to_insert = [fields[c] for c in cols_to_insert]

        if cols_to_insert:
            placeholders = ', '.join(['?'] * len(cols_to_insert))
            col_names = ', '.join(cols_to_insert)
            cursor.execute(
                f"INSERT INTO az_claims ({col_names}) VALUES ({placeholders})",
                vals_to_insert
            )

    conn.commit()
    conn.close()
    print(f"[LOCAL DB] Inserted {len(data_list)} az_claims record(s)")


def update_az_claims_local(az_claim_dict_list):
    """
    Local replacement for update_az_claims().
    Matches records by the composite key field_15 = '{order}-{asin}'.
    """
    conn = get_connection()
    cursor = conn.cursor()

    for claim in az_claim_dict_list:
        order_asin = f"{claim[9]}-{claim[10]}"

        # Find matching records
        cursor.execute("SELECT id FROM az_claims WHERE field_15 = ?", (order_asin,))
        rows = cursor.fetchall()

        for row in rows:
            record_id = row['id']
            for k, v in claim.items():
                if k not in (9, 10):
                    col = f"field_{k}"
                    cursor.execute(
                        f"UPDATE az_claims SET {col} = ? WHERE id = ?",
                        (str(v) if v is not None else None, record_id)
                    )

    conn.commit()
    conn.close()
    print(f"[LOCAL DB] Updated az_claims – {len(az_claim_dict_list)} claim(s) processed")


# ── Insert helpers (mirror db.py functions) ────────────────────────────────────

def insert_health(health):
    """Local replacement for db.insert_health()."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO account_health
            (storename, status, health_rating, odr, vtr, buybox, balance,
             negative_feedback, a_to_z_claims, chargeback_claims,
             late_shipment_rate, pre_fulfilment_cancel_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        health['storename'], health['status'], health['health_rating'],
        health['odr'], health['vtr'], health['buybox'], health['balance'],
        health['negative_feedback'], health['a_to_z_claims'],
        health['chargeback_claims'], health['late_shipment_rate'],
        health['pre_fulfilment_cancel_rate'],
    ))

    conn.commit()
    conn.close()
    print(f"[LOCAL DB] Inserted account_health for {health['storename']}")


def insert_violations(violations):
    """Local replacement for db.insert_violations()."""
    conn = get_connection()
    cursor = conn.cursor()

    data = [(v['storename'], v['asin'], v['impact'],
             v['action_taken'], v['reason'],
             str(v['publish_time']) if v['publish_time'] else None,
             v['category'])
            for v in violations]

    cursor.executemany("""
        INSERT OR IGNORE INTO listing_issues_sc
            (storename, asin, impact, action_taken, reason, publish_time, category)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, data)

    conn.commit()
    conn.close()
    print(f"[LOCAL DB] Inserted {len(violations)} violation(s)")


def get_distinct_storenames():
    """Local replacement for db.get_distinct_storenames()."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT storename FROM listing_issues_sc")
    result = [row['storename'] for row in cursor.fetchall()]
    conn.close()
    return result
