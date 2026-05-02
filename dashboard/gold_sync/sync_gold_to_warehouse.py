import logging
import os
import time
from typing import Dict

import duckdb
import pandas as pd
from sqlalchemy import create_engine, inspect, text


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s gold-sync - %(message)s",
)
logger = logging.getLogger("gold-sync")


GOLD_TABLES: Dict[str, str] = {
    "prediction_current": "s3://lakehouse/gold/prediction_current/",
    "prediction_history": "s3://lakehouse/gold/prediction_history/",
    "alert_current": "s3://lakehouse/gold/alert_current/",
    "alert_history": "s3://lakehouse/gold/alert_history/",
    "pipeline_quality": "s3://lakehouse/gold/pipeline_quality/",
    "prediction_current_phm": "s3://lakehouse/gold/prediction_current_phm/",
    "prediction_history_phm": "s3://lakehouse/gold/prediction_history_phm/",
    "alert_current_phm": "s3://lakehouse/gold/alert_current_phm/",
    "alert_history_phm": "s3://lakehouse/gold/alert_history_phm/",
    "pipeline_quality_phm": "s3://lakehouse/gold/pipeline_quality_phm/",
}


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _sql_type_for_series(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    return "TEXT"


def _ensure_table_schema(conn, table_name: str, df: pd.DataFrame) -> None:
    inspector = inspect(conn)
    if not inspector.has_table(table_name, schema="gold"):
        return

    existing_columns = {column["name"] for column in inspector.get_columns(table_name, schema="gold")}
    missing_columns = [column for column in df.columns if column not in existing_columns]

    if not missing_columns:
        return

    for column_name in missing_columns:
        sql_type = _sql_type_for_series(df[column_name])
        conn.execute(text(f'ALTER TABLE gold."{table_name}" ADD COLUMN IF NOT EXISTS "{column_name}" {sql_type};'))
        logger.info("Added missing column gold.%s.%s (%s)", table_name, column_name, sql_type)


def create_duckdb_connection() -> duckdb.DuckDBPyConnection:
    endpoint = _env("MINIO_S3_ENDPOINT", "minio:9000").replace("http://", "").replace(
        "https://", ""
    )
    access_key = _env("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = _env("MINIO_SECRET_KEY", "minioadmin123")
    use_ssl = _env("MINIO_USE_SSL", "false").lower() == "true"
    region = _env("MINIO_REGION", "us-east-1")

    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio_lakehouse (
          TYPE S3,
          KEY_ID '{access_key}',
          SECRET '{secret_key}',
          REGION '{region}',
          ENDPOINT '{endpoint}',
          URL_STYLE 'path',
          USE_SSL {'true' if use_ssl else 'false'}
        );
        """
    )
    con.execute(f"SET s3_region='{region}';")
    con.execute("SET s3_url_style='path';")
    return con


def fetch_delta_tables(con: duckdb.DuckDBPyConnection) -> Dict[str, pd.DataFrame]:
    loaded: Dict[str, pd.DataFrame] = {}
    for table_name, delta_path in GOLD_TABLES.items():
        try:
            query = f"SELECT * FROM delta_scan('{delta_path}')"
            df = con.execute(query).fetchdf()
            loaded[table_name] = df
            logger.info("Fetched %s rows from %s", len(df), table_name)
        except Exception as exc:
            logger.warning("Skip %s: %s", table_name, exc)
    return loaded


def write_to_postgres(frames: Dict[str, pd.DataFrame], dsn: str) -> None:
    if not frames:
        logger.warning("No Gold tables fetched. Nothing to sync.")
        return

    engine = create_engine(dsn, future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_overview_kpis;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_alert_level_counts;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_top_risk;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_low_rul;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_grafana_entrypoint;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_alert_counts_recent;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_alert_reason_parsed;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_symptom_details_parsed;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_machine_snapshot;"))
        
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_overview_kpis;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_alert_level_counts;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_top_risk;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_low_rul;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_grafana_entrypoint;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_machine_snapshot;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_alert_counts_recent;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_alert_reason_parsed;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_symptom_details_parsed;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_pipeline_quality_latest;"))
        conn.execute(text("DROP VIEW IF EXISTS gold.v_phm_latest_run;"))

        inspector = inspect(conn)
        for table_name, df in frames.items():
            target_name = f"gold_{table_name}"
            table_ref = f"gold.{target_name}"

            # Avoid transaction-abort cascade: check existence before TRUNCATE.
            if inspector.has_table(target_name, schema="gold"):
                _ensure_table_schema(conn, target_name, df)
                conn.execute(text(f"TRUNCATE TABLE {table_ref};"))
            else:
                df.head(0).to_sql(
                    target_name,
                    con=conn,
                    schema="gold",
                    if_exists="append",
                    index=False,
                )

            if not df.empty:
                df.to_sql(
                    target_name,
                    con=conn,
                    schema="gold",
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=2000,
                )
            logger.info("Upserted %s rows to gold.%s", len(df), target_name)


    # ── Views: each in its own connection so one failure doesn't abort the rest ──
    VIEW_SQLS = {
        "v_overview_kpis": """
            CREATE OR REPLACE VIEW gold.v_overview_kpis AS
            SELECT
              COUNT(*)::bigint AS total_machines,
              AVG(predicted_rul)::double precision AS avg_predicted_rul,
              MIN(predicted_rul)::double precision AS min_predicted_rul
            FROM gold.gold_prediction_current;
        """,
        "v_alert_level_counts": """
            CREATE OR REPLACE VIEW gold.v_alert_level_counts AS
            SELECT alert_level, COUNT(*)::bigint AS machine_count
            FROM gold.gold_alert_current
            GROUP BY alert_level;
        """,
        "v_top_risk": """
            CREATE OR REPLACE VIEW gold.v_top_risk AS
            SELECT unit_nr, alert_level, risk_score, pending_count, updated_at
            FROM gold.gold_alert_current
            ORDER BY risk_score DESC
            LIMIT 10;
        """,
        "v_low_rul": """
            CREATE OR REPLACE VIEW gold.v_low_rul AS
            SELECT unit_nr, window_end_cycle, predicted_rul, symptom_score, updated_at
            FROM gold.gold_prediction_current
            ORDER BY predicted_rul ASC
            LIMIT 10;
        """,
        "v_grafana_entrypoint": """
            CREATE OR REPLACE VIEW gold.v_grafana_entrypoint AS
            SELECT
              p.unit_nr,
              p.predicted_rul,
              a.risk_score,
              a.alert_level,
              a.updated_at
            FROM gold.gold_prediction_current p
            JOIN gold.gold_alert_current a USING (unit_nr)
            ORDER BY a.risk_score DESC, p.predicted_rul ASC;
        """,
        "v_alert_counts_recent": """
            CREATE OR REPLACE VIEW gold.v_alert_counts_recent AS
            SELECT
              date_trunc('minute', alert_time::timestamp) AS ts_minute,
              SUM(CASE WHEN alert_level = 'Warning' THEN 1 ELSE 0 END)::bigint AS warning_count,
              SUM(CASE WHEN alert_level = 'Critical' THEN 1 ELSE 0 END)::bigint AS critical_count
            FROM gold.gold_alert_history
            GROUP BY 1
            ORDER BY 1;
        """,
        "v_alert_reason_parsed": """
            CREATE OR REPLACE VIEW gold.v_alert_reason_parsed AS
            SELECT
              unit_nr,
              alert_time,
              alert_level,
              risk_score,
              rul_score,
              trend_score,
              symptom_score,
              (reason_json::jsonb ->> 'raw_level') AS raw_level,
              (reason_json::jsonb ->> 'confirmed_level') AS confirmed_level
            FROM gold.gold_alert_history;
        """,
        "v_symptom_details_parsed": """
            CREATE OR REPLACE VIEW gold.v_symptom_details_parsed AS
            SELECT
              unit_nr,
              prediction_time,
              sensor.key AS sensor_name,
              (sensor.value ->> 'deviation')::double precision AS deviation,
              (sensor.value ->> 'trend')::double precision AS trend,
              (sensor.value ->> 'volatility')::double precision AS volatility,
              (sensor.value ->> 'score')::double precision AS score
            FROM gold.gold_prediction_history,
            LATERAL jsonb_each(symptom_details_json::jsonb) AS sensor(key, value);
        """,
        "v_machine_snapshot": """
            CREATE OR REPLACE VIEW gold.v_machine_snapshot AS
            SELECT
              p.unit_nr,
              ROUND(p.predicted_rul::numeric, 2)  AS predicted_rul,
              ROUND(p.symptom_score::numeric, 4)  AS symptom_score,
              ROUND(p.trend_score::numeric, 4)    AS trend_score,
              p.window_end_cycle,
              ROUND(a.risk_score::numeric, 4)     AS risk_score,
              ROUND(a.rul_score::numeric, 4)      AS rul_score,
              a.alert_level,
              a.pending_count,
              a.updated_at
            FROM gold.gold_prediction_current p
            JOIN gold.gold_alert_current a USING (unit_nr);
        """,
        # ── PHM (IEEE 2012) views ──────────────────────────────────────────────
                "v_phm_latest_run": """
                        CREATE OR REPLACE VIEW gold.v_phm_latest_run AS
                        SELECT run_id
                        FROM gold.gold_prediction_history_phm
                        WHERE run_id IS NOT NULL
                        ORDER BY prediction_time DESC
                        LIMIT 1;
                """,
        "v_phm_overview_kpis": """
            CREATE OR REPLACE VIEW gold.v_phm_overview_kpis AS
            SELECT
              COUNT(*)::bigint AS total_machines,
              AVG(predicted_rul)::double precision AS avg_predicted_rul,
              MIN(predicted_rul)::double precision AS min_predicted_rul
                        FROM gold.gold_prediction_current_phm p
                        WHERE p.run_id = (SELECT run_id FROM gold.v_phm_latest_run);
        """,
        "v_phm_alert_level_counts": """
            CREATE OR REPLACE VIEW gold.v_phm_alert_level_counts AS
            SELECT alert_level, COUNT(*)::bigint AS machine_count
                        FROM gold.gold_alert_current_phm a
                        WHERE a.run_id = (SELECT run_id FROM gold.v_phm_latest_run)
            GROUP BY alert_level;
        """,
        "v_phm_top_risk": """
            CREATE OR REPLACE VIEW gold.v_phm_top_risk AS
            SELECT unit_nr, alert_level, risk_score, pending_count, updated_at
                        FROM gold.gold_alert_current_phm a
                        WHERE a.run_id = (SELECT run_id FROM gold.v_phm_latest_run)
            ORDER BY risk_score DESC
            LIMIT 10;
        """,
        "v_phm_low_rul": """
            CREATE OR REPLACE VIEW gold.v_phm_low_rul AS
            SELECT unit_nr, window_end_cycle, predicted_rul, symptom_score
                        FROM gold.gold_prediction_current_phm p
                        WHERE p.run_id = (SELECT run_id FROM gold.v_phm_latest_run)
            ORDER BY predicted_rul ASC
            LIMIT 10;
        """,
        "v_phm_grafana_entrypoint": """
            CREATE OR REPLACE VIEW gold.v_phm_grafana_entrypoint AS
            SELECT
              p.unit_nr,
              p.predicted_rul,
              a.risk_score,
              a.alert_level,
              a.updated_at
            FROM gold.gold_prediction_current_phm p
            JOIN gold.gold_alert_current_phm a USING (unit_nr)
                        WHERE p.run_id = (SELECT run_id FROM gold.v_phm_latest_run)
                            AND a.run_id = (SELECT run_id FROM gold.v_phm_latest_run)
            ORDER BY a.risk_score DESC, p.predicted_rul ASC;
        """,
        "v_phm_machine_snapshot": """
            CREATE OR REPLACE VIEW gold.v_phm_machine_snapshot AS
            SELECT
              p.unit_nr,
              ROUND(p.predicted_rul::numeric, 2)  AS predicted_rul,
              ROUND(p.symptom_score::numeric, 4)  AS symptom_score,
              p.window_end_cycle,
              ROUND(a.risk_score::numeric, 4)     AS risk_score,
              ROUND(a.rul_score::numeric, 4)      AS rul_score,
              a.alert_level,
              a.pending_count,
              a.updated_at
            FROM gold.gold_prediction_current_phm p
                        JOIN gold.gold_alert_current_phm a USING (unit_nr)
                        WHERE p.run_id = (SELECT run_id FROM gold.v_phm_latest_run)
                            AND a.run_id = (SELECT run_id FROM gold.v_phm_latest_run);
        """,
                "v_phm_alert_counts_recent": """
                        CREATE OR REPLACE VIEW gold.v_phm_alert_counts_recent AS
                        SELECT
                            date_trunc('minute', alert_time::timestamp) AS ts_minute,
                            SUM(CASE WHEN alert_level = 'Warning' THEN 1 ELSE 0 END)::bigint AS warning_count,
                            SUM(CASE WHEN alert_level = 'Critical' THEN 1 ELSE 0 END)::bigint AS critical_count
                        FROM gold.gold_alert_history_phm a
                        WHERE a.run_id = (SELECT run_id FROM gold.v_phm_latest_run)
                        GROUP BY 1
                        ORDER BY 1;
                """,
                "v_phm_alert_reason_parsed": """
                        CREATE OR REPLACE VIEW gold.v_phm_alert_reason_parsed AS
                        SELECT
                            unit_nr,
                            alert_time,
                            alert_level,
                            risk_score,
                            rul_score,
                            trend_score,
                            symptom_score,
                            (reason_json::jsonb ->> 'raw_level') AS raw_level,
                            (reason_json::jsonb ->> 'confirmed_level') AS confirmed_level
                        FROM gold.gold_alert_history_phm a
                        WHERE a.run_id = (SELECT run_id FROM gold.v_phm_latest_run);
                """,
                "v_phm_symptom_details_parsed": """
                        CREATE OR REPLACE VIEW gold.v_phm_symptom_details_parsed AS
                        SELECT
                            unit_nr,
                            prediction_time,
                            sensor.key AS sensor_name,
                            (sensor.value ->> 'deviation')::double precision AS deviation,
                            (sensor.value ->> 'trend')::double precision AS trend,
                            (sensor.value ->> 'volatility')::double precision AS volatility,
                            (sensor.value ->> 'score')::double precision AS score
                        FROM gold.gold_prediction_history_phm p,
                        LATERAL jsonb_each(symptom_details_json::jsonb) AS sensor(key, value)
                        WHERE p.run_id = (SELECT run_id FROM gold.v_phm_latest_run);
                """,
                "v_phm_pipeline_quality_latest": """
                        CREATE OR REPLACE VIEW gold.v_phm_pipeline_quality_latest AS
                        SELECT
                            quality_time,
                            run_id,
                            bronze_in_batch,
                            silver_total_records,
                            silver_valid_records,
                            silver_invalid_records,
                            CASE
                                WHEN silver_total_records = 0 THEN 0
                                ELSE silver_invalid_records::double precision / silver_total_records
                            END AS invalid_rate
                        FROM gold.gold_pipeline_quality_phm
                        WHERE run_id = (SELECT run_id FROM gold.v_phm_latest_run)
                        ORDER BY quality_time DESC
                        LIMIT 1;
                """,
    }

    for view_name, view_sql in VIEW_SQLS.items():
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP VIEW IF EXISTS gold.{view_name};"))
                conn.execute(text(view_sql))
            logger.info("Created view gold.%s", view_name)
        except Exception as exc:
            logger.warning("Skip view gold.%s (base table not ready yet): %s", view_name, exc)

    engine.dispose()


def main() -> None:
    dsn = _env(
        "WAREHOUSE_DSN",
        "postgresql+psycopg2://pdm:pdm@dashboard-db:5432/pdm_dashboard",
    )
    interval_sec = float(_env("SYNC_INTERVAL_SEC", "10"))
    initial_sleep = float(_env("INITIAL_SLEEP_SEC", "8"))

    logger.info("Starting Gold sync job. interval_sec=%s", interval_sec)
    time.sleep(initial_sleep)

    while True:
        try:
            con = create_duckdb_connection()
            frames = fetch_delta_tables(con)
            write_to_postgres(frames, dsn)
            con.close()
        except Exception as exc:
            logger.exception("Sync iteration failed: %s", exc)
        time.sleep(interval_sec)


if __name__ == "__main__":
    main()
