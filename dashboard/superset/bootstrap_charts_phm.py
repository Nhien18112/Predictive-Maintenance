#!/usr/bin/env python3
"""
Bootstrap Superset PHM Machine Investigation dashboard.

Layout
------
Row 0  – Native filter: unit_nr
Row 1  – KPI big-number cards (2 per row)
Row 2  – KPI big-number cards (2 per row)
Row 3  – RUL History | Risk Score History
Row 4  – Top Risk Machines (bar)
Row 5  – Current Status Table (full-width)
"""
import json
import sys

from superset.app import create_app


# ── Layout helpers ─────────────────────────────────────────────────────────────

def _row(row_name: str, chart_ids: list, col_widths: list | None = None) -> dict:
    if col_widths is None:
        w = 24 // len(chart_ids)
        col_widths = [w] * len(chart_ids)
    children = [f"CHART-{cid}" for cid in chart_ids]
    out = {
        row_name: {
            "type": "ROW",
            "id": row_name,
            "children": children,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
    }
    for cid, width in zip(chart_ids, col_widths):
        out[f"CHART-{cid}"] = {
            "type": "CHART",
            "id": f"CHART-{cid}",
            "children": [],
            "meta": {"chartId": cid, "height": 30, "width": width},
        }
    return out


def _build_layout(rows: list) -> str:
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
    }
    for row_name, chart_ids, widths in rows:
        layout["GRID_ID"]["children"].append(row_name)
        layout.update(_row(row_name, chart_ids, widths))
    return json.dumps(layout)


# ── Param factories ─────────────────────────────────────────────────────────────

def _big_number(dataset_id: int, col: str, agg: str = "AVG", subheader: str = "", fmt: str = ".2f") -> str:
    return json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SIMPLE",
            "column": {"column_name": col},
            "aggregate": agg,
            "label": col,
        },
        "subheader": subheader,
        "y_axis_format": fmt,
        "time_range": "No filter",
        "header_font_size": 0.4,
    })


def _big_number_sql(dataset_id: int, sql_expr: str, label: str, subheader: str = "", fmt: str = "SMART_NUMBER") -> str:
    return json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": sql_expr,
            "label": label,
        },
        "subheader": subheader,
        "y_axis_format": fmt,
        "time_range": "No filter",
        "header_font_size": 0.4,
    })


def _table(dataset_id: int, columns: list, row_limit: int = 100, order_col: str = "") -> str:
    p: dict = {
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "all_columns": columns,
        "row_limit": row_limit,
        "order_desc": True,
        "show_cell_bars": False,
        "time_range": "No filter",
    }
    if order_col:
        p["orderby"] = [[order_col, False]]
    return json.dumps(p)


def _line(dataset_id: int, x_axis: str, metrics: list) -> str:
    return json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "echarts_timeseries_line",
        "x_axis": x_axis,
        "time_grain_sqla": "PT1M",
        "time_range": "No filter",
        "metrics": metrics,
        "groupby": [],
        "row_limit": 5000,
        "rich_tooltip": True,
        "show_legend": True,
        "opacity": 0.8,
    })


def _bar_grouped(dataset_id: int, groupby: list, metrics: list) -> str:
    return json.dumps({
        "datasource": f"{dataset_id}__table",
        "viz_type": "dist_bar",
        "groupby": groupby,
        "columns": [],
        "metrics": metrics,
        "bar_stacked": False,
        "show_legend": False,
        "row_limit": 20,
        "order_desc": True,
        "time_range": "No filter",
        "y_axis_label": "Risk Score",
        "bottom_margin": "auto",
    })


def _m(col: str, agg: str = "AVG", label: str = "") -> dict:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label or col,
    }


def _m_sql(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


# ── Main ────────────────────────────────────────────────────────────────────────

def main() -> int:
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        db_obj = (
            db.session.query(Database)
            .filter(Database.database_name == "Gold Warehouse")
            .one_or_none()
        )
        if not db_obj:
            raise RuntimeError("Gold Warehouse not found in Superset")

        # ── 1. Ensure datasets ────────────────────────────────────────────────
        table_names = [
            "v_phm_machine_snapshot",
            "gold_prediction_history_phm",
            "gold_alert_history_phm",
            "v_phm_top_risk",
        ]
        datasets: dict = {}
        for t in table_names:
            ds = (
                db.session.query(SqlaTable)
                .filter(
                    SqlaTable.database_id == db_obj.id,
                    SqlaTable.schema == "gold",
                    SqlaTable.table_name == t,
                )
                .one_or_none()
            )
            if not ds:
                ds = SqlaTable(table_name=t, schema="gold", database_id=db_obj.id, sql=None)
                db.session.add(ds)
                db.session.commit()
            try:
                ds.fetch_metadata()
                db.session.commit()
            except Exception:
                db.session.rollback()
            datasets[t] = ds

        snap  = datasets["v_phm_machine_snapshot"].id
        phist = datasets["gold_prediction_history_phm"].id
        ahist = datasets["gold_alert_history_phm"].id
        top_r = datasets["v_phm_top_risk"].id

        # ── 2. Chart specs ────────────────────────────────────────────────────
        chart_specs = [
            # ── Row 1: KPI big-number cards ──────────────────────────────────
            ("KPI: PHM Predicted RUL",
             _big_number(snap, "predicted_rul", "AVG", "Predicted RUL (cycles)", ".1f")),
            ("KPI: PHM Risk Score",
             _big_number(snap, "risk_score", "AVG", "Risk Score (0-100)", ".1f")),
            ("KPI: PHM Alert Level",
             _big_number_sql(snap,
                "CASE MAX(alert_level) "
                "WHEN 'Critical' THEN 4 WHEN 'Warning' THEN 3 "
                "WHEN 'Watch' THEN 2 WHEN 'Normal' THEN 1 ELSE 0 END",
                "alert_level_num", "Alert Level (1=Normal, 2=Watch, 3=Warning, 4=Critical)", "d")),
            ("KPI: PHM Symptom Score",
             _big_number(snap, "symptom_score", "AVG", "Symptom Score (0–100)", ".1f")),

            # ── Row 3: history lines ──────────────────────────────────────────
            ("PHM RUL History (Line)",
             _line(phist, "prediction_time", [_m("predicted_rul")])),
            ("PHM Risk Score History (Line)",
             _line(ahist, "alert_time", [
                 _m("risk_score"), _m("rul_score"),
                 _m("trend_score"), _m("symptom_score"),
             ])),

            # ── Row 4: top risk bar ──────────────────────────────────────────
            ("PHM Top Risk (Bar)",
             _bar_grouped(top_r, ["unit_nr"], [_m("risk_score")])),

            # ── Row 2: Status table ──────────────────────────────────────────
            ("PHM Status Table",
             _table(snap,
                    ["unit_nr", "predicted_rul", "alert_level", "risk_score", "updated_at"],
                    row_limit=200, order_col="risk_score")),
        ]

        # ── 3. Delete stale slices then recreate ──────────────────────────────
        for name, _ in chart_specs:
            for stale in db.session.query(Slice).filter(Slice.slice_name == name).all():
                db.session.delete(stale)
        db.session.commit()

        charts: dict = {}
        for name, params in chart_specs:
            parsed = json.loads(params)
            viz    = parsed.get("viz_type", "table")
            ds_str = parsed.get("datasource", "")
            ds_id  = int(ds_str.split("__")[0]) if "__" in ds_str else 0
            slc = Slice(slice_name=name, datasource_type="table",
                        datasource_id=ds_id, viz_type=viz, params=params)
            db.session.add(slc)
            db.session.commit()
            charts[name] = slc

        # ── 4. Assign to dashboard ────────────────────────────────────────────
        md_dash = (
            db.session.query(Dashboard)
            .filter(Dashboard.slug == "phm-machine-detail")
            .one_or_none()
        )
        if not md_dash:
            raise RuntimeError("PHM Machine Investigation dashboard not found – run bootstrap_dashboard_phm.py first")

        ordered = [
            charts["KPI: PHM Predicted RUL"],
            charts["KPI: PHM Risk Score"],
            charts["KPI: PHM Alert Level"],
            charts["KPI: PHM Symptom Score"],
            charts["PHM RUL History (Line)"],
            charts["PHM Risk Score History (Line)"],
            charts["PHM Top Risk (Bar)"],
            charts["PHM Status Table"],
        ]
        ids = [c.id for c in ordered]

        position = _build_layout([
            # Row 1: KPI cards (2 per row)
            ("ROW-1", ids[0:2], [12, 12]),
            # Row 2: KPI cards (2 per row)
            ("ROW-2", ids[2:4], [12, 12]),
            # Row 3: history lines
            ("ROW-3", ids[4:6], [12, 12]),
            # Row 4: top risk bar
            ("ROW-4", ids[6:7], [24]),
            # Row 5: Status table full-width
            ("ROW-5", ids[7:8], [24]),
        ])

        # ── 5. Native filter for unit_nr ──────────────────────────────────────
        snap_ds_id = datasets["v_phm_machine_snapshot"].id
        native_filter = {
            "id": "NATIVE_FILTER-pdm-phm-unit-nr",
            "name": "Machine (unit_nr)",
            "filterType": "filter_select",
            "targets": [{"datasetId": snap_ds_id, "column": {"name": "unit_nr"}}],
            "defaultDataMask": {"filterState": {"value": None}},
            "controlValues": {
                "multiSelect": False,
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "inverseSelection": False,
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "description": "Select a PHM machine to investigate",
        }
        metadata = json.dumps({
            "color_scheme": "supersetColors",
            "expanded_slices": {},
            "label_colors": {},
            "native_filter_configuration": [native_filter],
            "filter_scopes": {},
        })

        md_dash.slices = ordered
        md_dash.position_json = position
        md_dash.json_metadata  = metadata
        md_dash.published = True
        db.session.commit()

        print("PHM Machine Investigation dashboard bootstrapped successfully.")
        print(f"  Charts ({len(ordered)}): {[c.slice_name for c in ordered]}")
        print("  Native filter: unit_nr")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"bootstrap_charts_phm failed: {exc}", file=sys.stderr)
        raise
