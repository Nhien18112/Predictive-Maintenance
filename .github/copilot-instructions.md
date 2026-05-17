# Copilot instructions

## Run PDM and PHM streams together
When asked to run both PDM (CMAPSS) and PHM pipelines at the same time, use this sequence and keep the replay steps in separate terminals:

1) Start core + PDM ingest
- `./run.ps1 -Action up-ingest`

2) Start PDM bronze
- `./run.ps1 -Action up-bronze`

3) Start PDM ops (silver -> gold -> inference)
- `./run.ps1 -Action up-ops`

4) Start PHM pipeline (ingest + bronze + ops + mlflow)
- `./run.ps1 -Action up-phm`

5) Start dashboards (optional but common)
- `./run.ps1 -Action up-dashboard`

6) Replay data in parallel (use two terminals)
- Terminal A (PDM): `./run.ps1 -Action replay -CsvPath "Data/raw_streaming.csv" -ReplayMode event-time -ReplaySpeed 0.2`
- Terminal B (PHM): `./run.ps1 -Action replay-phm`

Notes:
- If Kafka or EMQX are not ready, the script already waits, so do not add extra waits.
- Use `./run.ps1 -Action logs -Service <service> -Follow` for troubleshooting.
- To stop all services: `./run.ps1 -Action down-all`.

## Run end-to-end (PDM only)
Use this when the user wants a single, simple end-to-end flow for the CMAPSS/PDM pipeline.

1) Start core + ingest + bronze + ops
- `./run.ps1 -Action up-all`

2) Start dashboards
- `./run.ps1 -Action up-dashboard`

3) Replay data (new terminal)
- `./run.ps1 -Action replay -CsvPath "Data/raw_streaming.csv" -ReplayMode event-time -ReplaySpeed 0.2`
