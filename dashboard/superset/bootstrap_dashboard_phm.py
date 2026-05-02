#!/usr/bin/env python3
"""
Bootstrap Superset PHM Machine Investigation dashboard.
"""
import sys
from superset.app import create_app

def main() -> int:
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.dashboard import Dashboard

        dash_slug = "phm-machine-detail"
        dash_title = "PHM Machine Investigation"

        existing = db.session.query(Dashboard).filter(Dashboard.slug == dash_slug).one_or_none()
        if existing:
            existing.dashboard_title = dash_title
            existing.published = True
        else:
            d = Dashboard(
                dashboard_title=dash_title,
                slug=dash_slug,
                published=True,
                position_json="{}",
                json_metadata="{}",
            )
            db.session.add(d)

        db.session.commit()
    return 0

if __name__ == "__main__":
    sys.exit(main())
