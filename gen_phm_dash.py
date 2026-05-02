import re
import os

source_file = 'dashboard/grafana/dashboards/overview.json'
target_file = 'dashboard/grafana/dashboards/overview_phm.json'

with open(source_file, 'r', encoding='utf-8') as f:
    text = f.read()

# Thay thế các view sang phiên bản PHM
text = text.replace('v_overview_kpis', 'v_phm_overview_kpis')
text = text.replace('v_alert_level_counts', 'v_phm_alert_level_counts')
text = text.replace('v_top_risk', 'v_phm_top_risk')
text = text.replace('v_low_rul', 'v_phm_low_rul')
text = text.replace('v_grafana_entrypoint', 'v_phm_grafana_entrypoint')

# Fix panel titles and UIDs
text = text.replace('"title": "PDM Gold Overview"', '"title": "PHM Gold Overview"')
text = text.replace('"uid": "pdm-gold-overview"', '"uid": "phm-gold-overview"')

with open(target_file, 'w', encoding='utf-8') as f:
    f.write(text)

print('Generated overview_phm.json')
