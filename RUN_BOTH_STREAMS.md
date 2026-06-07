# Run both PDM and PHM streams

This guide runs both pipelines together, avoiding repeated startup for shared services. Use PowerShell at the repo root.

## 0) One-time (per terminal session)
```powershell
Set-ExecutionPolicy Bypass -Scope Process
```

## 1) Start shared core services (EMQX + Kafka)
```powershell
./run.ps1 -Action up-core
```

## 2) Start PDM ingest + bronze + ops
```powershell
./run.ps1 -Action up-ingest
./run.ps1 -Action up-bronze
./run.ps1 -Action up-ops
```

## 3) Start PHM pipeline (ingest + bronze + ops + MLflow)
*Note: Running `up-phm` will **automatically reset and clean** any previous PHM data from MinIO, PostgreSQL, and Kafka to ensure a fresh start.*
```powershell
./run.ps1 -Action up-phm
```
*(If you ever need to manually clean the PHM data without starting the services, use `./run.ps1 -Action clean-phm`)*

## 4) Start dashboards
```powershell
./run.ps1 -Action up-dashboard
```

## 5) Replay data in parallel (two terminals)
Terminal A (PDM):
```powershell
./run.ps1 -Action replay -CsvPath "Data/raw_streaming.csv" -ReplayMode event-time -ReplaySpeed 0.2
```

Terminal B (PHM):
```powershell
./run.ps1 -Action replay-phm
```

## 6) Quick checks after everything is up
Service URLs:
- Kafka UI: http://localhost:8080
- MinIO Console: http://localhost:9001
- Grafana: http://localhost:3000 (admin/admin)
- Superset: http://localhost:8088 (admin/admin)
- EMQX Dashboard: http://localhost:18083

Health/log checks:
```powershell
./run.ps1 -Action health
./run.ps1 -Action logs -Service mqtt-kafka-bridge -Follow
./run.ps1 -Action logs -Service bronze-telemetry -Follow
./run.ps1 -Action logs -Service silver-gold-inference-alert -Follow
./run.ps1 -Action logs -Service gold-sync -Follow
```

Data checks:
- Kafka UI: topic `pdm.fd001.raw` and `pdm.phm.raw` should have messages.
- MinIO: bucket `lakehouse` should show bronze/silver/gold folders.
- Grafana: open dashboard "PDM Gold Overview" and "PHM Gold Overview".
- Superset: if charts are empty, run `./run.ps1 -Action refresh-superset` after at least one gold-sync cycle.

## Stop everything
```powershell
./run.ps1 -Action down-all
```

## Troubleshooting (common)
- Follow logs: `./run.ps1 -Action logs -Service <service> -Follow`
- If Superset charts are empty: run `./run.ps1 -Action refresh-superset` after data exists.


Restart your Docker containers so the Superset fix applies:

powershell
./run.ps1 -Action down-dashboard
./run.ps1 -Action up-dashboard
Open a new terminal and install the new Flask requirement:

powershell
pip install -r requirements.txt
Start your new web portal server!

powershell
cd ui-portal
python app.py
Go to http://localhost:5000 in your web browser. You'll be presented with your new login screen!

Username: admin
Password: password123