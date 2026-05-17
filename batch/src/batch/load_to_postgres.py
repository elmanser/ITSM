import os
from datetime import timedelta

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env"))

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB   = os.getenv("POSTGRES_DB", "itsm_dw")
PG_USER = os.getenv("POSTGRES_USER", "itsm")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "itsm_dw_secret_2026")

STATUS_MAP = {"Closed": 6, "Open": 2}
PRIORITY_MAP = {
    "1 - Haute": 2, "2 - Moyenne": 3, "3 - Faible": 4,
    "4": 4, "5": 5, "3": 3, "2": 2, "1": 1,
}
SLA_LIMITS = {1: 4, 2: 8, 3: 24, 4: 72, 5: 168}


def _get_or_create(cur, table: str, id_col: str, name_col: str,
                   name: str, extra_col: str = None, extra_val: str = None) -> int | None:
    if not name or str(name).strip() in ("", "nan", "-"):
        return None
    name = str(name).strip()
    cur.execute(f"SELECT {id_col} FROM {table} WHERE {name_col} = %s LIMIT 1", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    if extra_col:
        cur.execute(
            f"INSERT INTO {table} ({name_col}, {extra_col}) VALUES (%s, %s) RETURNING {id_col}",
            (name, extra_val),
        )
    else:
        cur.execute(
            f"INSERT INTO {table} ({name_col}) VALUES (%s) RETURNING {id_col}",
            (name,),
        )
    return cur.fetchone()[0]


def _parse_priority(val) -> int:
    if not val or str(val) == "nan":
        return 3
    v = str(val).strip()
    for k, code in PRIORITY_MAP.items():
        if k in v:
            return code
    try:
        return int(float(v))
    except ValueError:
        return 3


def load_data():
    data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data", "curated")
    csv_path = os.path.join(data_dir, "fact_tickets.csv")

    print(f"Connecting to PostgreSQL {PG_HOST}:{PG_PORT}/{PG_DB}...")
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                            user=PG_USER, password=PG_PASS)
    conn.autocommit = False
    cur = conn.cursor()

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} records from batch CSV.")

    ticket_records = []
    sla_data = []       # (ticket_id_val, created_at, closed_at, mttr, priority_code)

    for _, row in df.iterrows():
        priority_code = _parse_priority(row.get("priority"))
        status_code   = STATUS_MAP.get(str(row.get("normalized_status", "Open")).strip(), 2)

        created_at = row["created_at"] if pd.notna(row.get("created_at")) else None
        closed_at  = row["closed_at"]  if pd.notna(row.get("closed_at"))  else None

        mttr = row.get("resolution_time_hours")
        mttr = float(mttr) if pd.notna(mttr) and float(mttr) >= 0 else None

        urgency_raw = str(row.get("urgency", "3"))
        urgency = int(urgency_raw.split(" - ")[0]) if " - " in urgency_raw else 3

        # Resolve category
        cat_name = str(row.get("category_full", "")).strip()
        category_id = _get_or_create(cur, "dim_category", "category_id", "name",
                                     cat_name, "itil_type", "incident")

        # Resolve group
        grp_name = str(row.get("resolver_group", "")).strip()
        group_id = _get_or_create(cur, "dim_group", "group_id", "name",
                                  grp_name, "team_type", "support")

        ticket_records.append((
            str(row["ticket_id"]),
            cat_name,
            str(row.get("description", "")),
            priority_code, status_code,
            category_id, group_id,
            urgency, 3,
            created_at, closed_at, closed_at,
            mttr, None, "csv_batch",
        ))

        if mttr is not None and created_at is not None:
            sla_data.append((str(row["ticket_id"]), created_at, closed_at, mttr, priority_code))

    # Upsert fact_tickets
    insert_tickets = """
        INSERT INTO fact_tickets (
            glpi_ticket_id, title, description, priority_id, status_id,
            category_id, group_id,
            urgency, impact, date_creation, date_resolution, date_close,
            mttr_hours, sla_respected, source, ingested_at, updated_at
        )
        VALUES (
            %s, %s, %s,
            (SELECT priority_id FROM dim_priority WHERE code = %s LIMIT 1),
            (SELECT status_id  FROM dim_status   WHERE code = %s LIMIT 1),
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, NOW(), NOW()
        )
        ON CONFLICT (glpi_ticket_id) DO UPDATE SET
            title           = EXCLUDED.title,
            description     = EXCLUDED.description,
            priority_id     = EXCLUDED.priority_id,
            status_id       = EXCLUDED.status_id,
            category_id     = EXCLUDED.category_id,
            group_id        = EXCLUDED.group_id,
            date_resolution = EXCLUDED.date_resolution,
            date_close      = EXCLUDED.date_close,
            mttr_hours      = EXCLUDED.mttr_hours,
            source          = EXCLUDED.source,
            updated_at      = NOW()
        RETURNING glpi_ticket_id, ticket_id;
    """
    print("Upserting fact_tickets with category + group...")
    # execute_batch doesn't return RETURNING — use executemany via cursor loop for id map
    ticket_id_map = {}
    for rec in ticket_records:
        cur.execute(insert_tickets, rec)
        row = cur.fetchone()
        if row:
            ticket_id_map[row[0]] = row[1]   # glpi_ticket_id → ticket_id

    conn.commit()
    print(f"  >> {len(ticket_id_map)} tickets upserted.")

    # Insert SLA records
    print("Inserting SLA records for batch tickets...")
    sla_inserted = 0
    for (glpi_id, created_at, resolution_date, mttr, priority_code) in sla_data:
        ticket_id = ticket_id_map.get(glpi_id)
        if not ticket_id:
            continue
        limit_h = SLA_LIMITS.get(priority_code, 24)
        try:
            from datetime import datetime
            created_dt = pd.to_datetime(created_at)
            resolution_dt = pd.to_datetime(resolution_date)
            deadline = created_dt + timedelta(hours=limit_h)
            sla_respected = mttr <= limit_h
            delay = round((resolution_dt - deadline).total_seconds() / 3600, 2)
        except Exception:
            continue

        cur.execute("""
            INSERT INTO fact_ticket_sla
                (ticket_id, sla_deadline, resolution_date, sla_respected, delay_hours, mttr_hours)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (ticket_id, deadline, resolution_dt, sla_respected, delay, mttr))
        sla_inserted += 1

    conn.commit()
    print(f"  >> {sla_inserted} SLA records inserted.")

    cur.close()
    conn.close()
    print("Done! Batch data fully loaded into the Data Warehouse.")


if __name__ == "__main__":
    load_data()
