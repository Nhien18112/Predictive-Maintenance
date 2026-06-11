param(
    [ValidateSet(
        "install",
        "split-train-stream",
        "up-core", "down-core",
        "up-ingest", "down-ingest",
        "up-bronze", "down-bronze",
        "up-ops", "down-ops",
        "up-dashboard", "down-dashboard", "refresh-superset",
        "build-train-silver-gold", "down-train",
        "up-all", "down-all", "up-phm", "down-phm", "train-phm", "clean-phm", "train-nasa",
        "replay", "replay-phm",
        "consume-raw", "consume-dlq",
        "logs", "status", "health",
        "up", "bridge", "bronze", "down", "all"
    )]
    [string]$Action = "status",

    [string]$CsvPath = "Data/raw_streaming.csv",
    [string]$FullLifecycleCsv = "Data/full_lifecycle_fd001.csv",
    [string]$TrainHistoryCsv = "Data/train_history.csv",
    [string]$RawStreamingCsv = "Data/raw_streaming.csv",
    [double]$TrainRatio = 0.7,
    [int]$SplitSeed = 42,
    [string]$StreamingBaseTime = "2026-01-01 00:00:00",
    [string]$Broker = "localhost",
    [int]$Port = 18831,
    [ValidateSet(0, 1, 2)]
    [int]$Qos = 1,

    [ValidateSet("fixed", "event-time")]
    [string]$ReplayMode = "event-time",
    [double]$FixedIntervalSeconds = 0.1,
    [double]$ReplaySpeed = 20.0,
    [int]$MaxRows = 0,

    [string]$MqttTopic = "factory/pdm/fd001/raw",
    [string]$KafkaBootstrap = "localhost:9092",
    [string]$RawTopic = "pdm.fd001.raw",
    [string]$DlqTopic = "pdm.fd001.raw.dlq",

    [ValidateSet("emqx", "kafka", "kafka-ui", "mqtt-kafka-bridge", "minio", "minio-init", "bronze-telemetry", "silver-gold-inference-alert", "train-silver-gold", "dashboard-db", "gold-sync", "superset", "grafana")]
    [string]$Service = "mqtt-kafka-bridge",
    [switch]$Follow,
    [int]$Tail = 120
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$script:PythonExe = $null

# docker compose only resolves profiled services when those profiles are enabled (ps/logs/exec).
$script:ComposeProfileCore = @("--profile", "core")
$script:ComposeProfileCoreIngest = @("--profile", "core", "--profile", "ingest")
$script:ComposeProfileAll = @(
    "--profile", "core", "--profile", "ingest", "--profile", "bronze",
    "--profile", "ops", "--profile", "train", "--profile", "dashboard",
    "--profile", "ingest-phm", "--profile", "bronze-phm", "--profile", "ops-phm"
)

function Write-Step([string]$Message) {
    Write-Host "`n=== $Message ===" -ForegroundColor Cyan
}

function Ensure-Command([string]$CommandName) {
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Missing required command: $CommandName"
    }
}

function Get-PythonExe {
    if ($script:PythonExe) {
        return $script:PythonExe
    }

    if ($env:VIRTUAL_ENV) {
        $activeVenvPython = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
        if (Test-Path $activeVenvPython) {
            $script:PythonExe = $activeVenvPython
            return $script:PythonExe
        }
    }

    $repoVenvPython = Join-Path $PSScriptRoot "venv\Scripts\python.exe"
    if (Test-Path $repoVenvPython) {
        $script:PythonExe = $repoVenvPython
        return $script:PythonExe
    }

    $cmd = Get-Command "python" -ErrorAction SilentlyContinue
    if ($cmd) {
        $script:PythonExe = $cmd.Source
        return $script:PythonExe
    }

    throw "Missing required command: python"
}

function Invoke-Checked([scriptblock]$Script, [string]$ErrorMessage) {
    & $Script
    if ($LASTEXITCODE -ne 0) {
        throw $ErrorMessage
    }
}

function Remove-ConflictingContainers([string[]]$Names) {
    # Disabled by request: do not force-remove existing containers.
    # Keep function in place so existing calls remain compatible.
    return
}



function Install-Dependencies {
    Write-Step "Installing Python dependencies"
    $pythonExe = Get-PythonExe
    Invoke-Checked { & $pythonExe -m pip install -r "requirements.txt" } "Failed to install Python dependencies"
}

function Compose-UpCore {
    Write-Step "Starting CORE stage (EMQX + Kafka + Kafka UI)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("kafka", "emqx", "kafka-ui")
    Invoke-Checked { docker compose --profile core up -d emqx kafka kafka-ui } "Failed to start core stage"
}

function Compose-DownCore {
    Write-Step "Stopping CORE stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core stop emqx kafka kafka-ui } "Failed to stop core stage"
}

function Wait-KafkaReady([int]$TimeoutSeconds = 90) {
    Write-Step "Waiting for Kafka readiness"
    Ensure-Command "docker"

    $start = Get-Date
    while (((Get-Date) - $start).TotalSeconds -lt $TimeoutSeconds) {
        $topics = ""
        try {
            $topics = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>&1
        } catch {
            # Ignore errors while waiting
        }
        if ($LASTEXITCODE -eq 0 -and $topics -notmatch "Exception") {
            Write-Host "Kafka is ready" -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for Kafka"
}

function Ensure-KafkaTopic([string]$TopicName, [int]$Partitions = 4, [int]$Retries = 8, [int]$DelaySeconds = 2) {
    Ensure-Command "docker"

    for ($attempt = 1; $attempt -le $Retries; $attempt++) {
        $topics = ""
        try {
            $topics = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>&1
        } catch {}
        
        if ($LASTEXITCODE -ne 0 -or $topics -match "Exception") {
            Write-Host "Kafka metadata not ready (attempt $attempt/$Retries), retrying..." -ForegroundColor Yellow
            Start-Sleep -Seconds $DelaySeconds
            continue
        }

        if (($topics -split "`r?`n") -contains $TopicName) {
            Write-Host "Topic '$TopicName' is ready" -ForegroundColor Green
            return
        }

        try {
            docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
                --bootstrap-server kafka:9092 `
                --create --if-not-exists `
                --topic $TopicName `
                --partitions $Partitions `
                --replication-factor 1 2>&1 | Out-Null
        } catch {}

        if ($LASTEXITCODE -eq 0) {
            $topicsAfterCreate = ""
            try {
                $topicsAfterCreate = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>&1
            } catch {}
            if ($LASTEXITCODE -eq 0 -and (($topicsAfterCreate -split "`r?`n") -contains $TopicName)) {
                Write-Host "Ensured topic '$TopicName'" -ForegroundColor Green
                return
            }
        }

        Write-Host "Topic '$TopicName' not ready yet (attempt $attempt/$Retries), retrying..." -ForegroundColor Yellow
        Start-Sleep -Seconds $DelaySeconds
    }

    throw "Failed to ensure topic '$TopicName' after $Retries attempts"
}

function Wait-EmqxHealthy([int]$TimeoutSeconds = 90) {
    Write-Step "Waiting for EMQX healthy state"
    Ensure-Command "docker"

    $start = Get-Date
    while (((Get-Date) - $start).TotalSeconds -lt $TimeoutSeconds) {
        $id = docker compose @ComposeProfileCore ps -q emqx 2>$null
        if ($id) {
            $status = docker inspect --format "{{.State.Health.Status}}" $id 2>$null
            if ($LASTEXITCODE -eq 0 -and $status -eq "healthy") {
                Write-Host "EMQX is healthy" -ForegroundColor Green
                return
            }
        }
        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for EMQX"
}

function Ensure-KafkaTopics {
    Write-Step "Ensuring Kafka topics ($RawTopic, $DlqTopic)"
    Ensure-Command "docker"

    Ensure-KafkaTopic -TopicName $RawTopic -Partitions 4
    Ensure-KafkaTopic -TopicName $DlqTopic -Partitions 1
}

function Start-Ingest {
    Write-Step "Starting INGEST stage (MQTT -> Kafka bridge)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("mqtt-kafka-bridge")

    Invoke-Checked { docker compose --profile core --profile ingest up -d mqtt-kafka-bridge } "Failed to start ingest stage"
    Write-Host "Follow logs with: docker compose --profile core --profile ingest logs -f mqtt-kafka-bridge" -ForegroundColor Yellow
}

function Stop-Ingest {
    Write-Step "Stopping INGEST stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile ingest stop mqtt-kafka-bridge } "Failed to stop ingest stage"
}

function Start-Bronze {
    Write-Step "Starting BRONZE stage (MinIO + Spark Structured Streaming)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("minio", "minio-init", "bronze-telemetry")

    Invoke-Checked { docker compose --profile core --profile bronze up -d minio minio-init bronze-telemetry } "Failed to start bronze stage"
    Write-Host "MinIO console: http://localhost:9001 (minioadmin / minioadmin123)" -ForegroundColor Yellow
    Write-Host "Follow Bronze logs with: docker logs -f bronze-telemetry" -ForegroundColor Yellow
}

function Stop-Bronze {
    Write-Step "Stopping BRONZE stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile bronze stop bronze-telemetry minio-init minio } "Failed to stop bronze stage"
}

function Start-Ops {
    Write-Step "Starting OPS stage (Silver -> Gold -> Inference -> Alert)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("silver-gold-inference-alert")
    Invoke-Checked { docker compose --profile core --profile bronze --profile ops up -d --no-deps silver-gold-inference-alert } "Failed to start ops stage"
}

function Stop-Ops {
    Write-Step "Stopping OPS stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile bronze --profile ops stop silver-gold-inference-alert } "Failed to stop ops stage"
}

function Start-Dashboard {
    Write-Step "Starting DASHBOARD stage (Grafana + Superset + Gold sync)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("dashboard-db", "gold-sync", "superset", "grafana")

    Invoke-Checked { docker compose --profile core --profile bronze --profile dashboard up -d dashboard-db gold-sync superset grafana } "Failed to start dashboard services"
    Invoke-Checked { docker compose --profile core --profile bronze --profile dashboard run --rm superset-init } "Failed to initialize Superset"

    Write-Host "Grafana: http://localhost:3000 (admin/admin)" -ForegroundColor Yellow
    Write-Host "Superset: http://localhost:8088 (admin/admin)" -ForegroundColor Yellow
}

function Stop-Dashboard {
    Write-Step "Stopping DASHBOARD stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile dashboard stop grafana superset gold-sync dashboard-db } "Failed to stop dashboard stage"
}

function Refresh-SupersetMeta {
    Write-Step "Refreshing Superset dataset metadata from Gold Warehouse"
    Ensure-Command "docker"
    $container = docker ps --filter "name=superset" --filter "status=running" --format "{{.Names}}" | Select-Object -First 1
    if (-not $container) {
        Write-Host "[warn] Superset container is not running. Start dashboard first with: .\run.ps1 -Action up-dashboard" -ForegroundColor Yellow
        return
    }
    Write-Host "Running metadata refresh in container: $container" -ForegroundColor Cyan
    docker exec $container python3 /app/dashboard/superset/refresh_metadata.py
    Write-Host "Superset metadata refreshed. Open http://localhost:8088 and Ctrl+F5." -ForegroundColor Green
}

function Build-TrainSilverGold {
    Write-Step "Building TRAIN Silver/Gold datasets to MinIO"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("minio", "minio-init", "train-silver-gold")

    Invoke-Checked { docker compose --profile train up -d minio minio-init } "Failed to start MinIO for train datasets"
    Invoke-Checked { docker compose --profile train run --rm train-silver-gold } "Failed to build train silver/gold datasets"
}

function Run-SplitTrainStream {
    Write-Step "Splitting full-life-cycle data into physical train/stream CSV files"
    $pythonExe = Get-PythonExe

    if (-not (Test-Path $FullLifecycleCsv)) {
        throw "Full lifecycle CSV not found: $FullLifecycleCsv"
    }

    Invoke-Checked {
        & $pythonExe "scripts/split_train_stream_files.py" `
            --input-csv "$FullLifecycleCsv" `
            --train-output-csv "$TrainHistoryCsv" `
            --stream-output-csv "$RawStreamingCsv" `
            --train-ratio $TrainRatio `
            --seed $SplitSeed `
            --stream-base-time "$StreamingBaseTime"
    } "Failed to split full lifecycle dataset"
}

function Stop-TrainStage {
    Write-Step "Stopping TRAIN stage services"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile train stop train-silver-gold minio-init minio } "Failed to stop train stage"
}

function Wait-BridgeReady([int]$TimeoutSeconds = 600) {
    Write-Step "Waiting for bridge MQTT subscription"
    Ensure-Command "docker"

    $start = Get-Date
    while (((Get-Date) - $start).TotalSeconds -lt $TimeoutSeconds) {
        # mqtt-kafka-bridge is profile "ingest" only — ps -q without --profile ingest returns nothing.
        $bridgeId = docker compose @ComposeProfileCoreIngest ps -q mqtt-kafka-bridge 2>$null
        if (-not $bridgeId) {
            Start-Sleep -Seconds 2
            continue
        }
        $running = docker inspect --format "{{.State.Running}}" $bridgeId 2>$null
        if ($LASTEXITCODE -ne 0 -or $running -ne "true") {
            Start-Sleep -Seconds 2
            continue
        }

        $logs = ""
        try {
            # Use docker logs (not compose logs): profiled services may not show logs via
            # `docker compose logs` unless every call repeats --profile flags.
            $logs = docker logs --tail 200 $bridgeId 2>&1 | Out-String
        } catch {
            Start-Sleep -Seconds 2
            continue
        }

        if ($logs -match "ERROR:\s*MQTT connect failed") {
            throw "MQTT bridge failed to connect to EMQX. Last logs:`n$logs"
        }

        if ($logs -match "Connected MQTT and subscribed to topic=") {
            Write-Host "Bridge is subscribed and ready" -ForegroundColor Green
            return
        }

        Start-Sleep -Seconds 2
    }

    $tail = ""
    try {
        $bid = docker compose @ComposeProfileCoreIngest ps -q mqtt-kafka-bridge 2>$null
        if ($bid) { $tail = docker logs --tail 120 $bid 2>&1 | Out-String }
    } catch { }

    throw "Timed out waiting for bridge MQTT subscription. Last logs:`n$tail"
}

function Run-Replay {
    Write-Step "Replaying CSV to MQTT"
    $pythonExe = Get-PythonExe

    if (-not (Test-Path $CsvPath)) {
        throw "CSV file not found: $CsvPath"
    }

    Invoke-Checked {
        & $pythonExe "simulator/replay_mqtt_from_csv.py" `
            --csv "$CsvPath" `
            --broker "$Broker" `
            --port $Port `
            --qos $Qos `
            --topic "$MqttTopic" `
            --replay-mode $ReplayMode `
            --fixed-interval-seconds $FixedIntervalSeconds `
            --replay-speed $ReplaySpeed `
            --max-rows $MaxRows
    } "Replay failed"
}

function Run-ReplayPhm {
    Write-Step "Replaying PHM MQTT (all bearings in parallel)"
    $pythonExe = Get-PythonExe
    $projectRoot = $PSScriptRoot   # Absolute path to the project directory
    $testSetPath = Join-Path $projectRoot "Data/phm-ieee-2012-data-challenge-dataset-master/Full_Test_Set"
    $scriptPath  = Join-Path $projectRoot "simulator/replay_phm_mqtt.py"
    $runId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $bearingFolders = Get-ChildItem $testSetPath -Directory | Select-Object -ExpandProperty FullName

    if (-not $bearingFolders) {
        throw "No bearing folders found in $testSetPath"
    }

    Write-Host "Found $($bearingFolders.Count) bearing(s): $($bearingFolders | ForEach-Object { Split-Path $_ -Leaf })" -ForegroundColor Cyan
    Write-Host "Run ID: $runId" -ForegroundColor Cyan

    $jobs = @()
    foreach ($folder in $bearingFolders) {
        $bearingName = Split-Path $folder -Leaf
        Write-Host "  Starting replay for $bearingName..." -ForegroundColor Yellow
        $jobs += Start-Job -ScriptBlock {
            param($py, $script, $bf, $port, $root, $run)
            Set-Location $root
            & $py $script --bearing-folder $bf --port $port --delay 1.0 --run-id $run
        } -ArgumentList $pythonExe, $scriptPath, $folder, 18831, $projectRoot, $runId
    }

    Write-Host "All $($jobs.Count) bearing simulators running. Press Ctrl+C to stop." -ForegroundColor Green
    try {
        # Stream output from all jobs until all complete
        while ($jobs | Where-Object { $_.State -eq 'Running' }) {
            $jobs | Receive-Job | ForEach-Object { Write-Host $_ }
            Start-Sleep -Seconds 2
        }
        $jobs | Receive-Job | ForEach-Object { Write-Host $_ }
    } finally {
        $jobs | Stop-Job -ErrorAction SilentlyContinue
        $jobs | Remove-Job -ErrorAction SilentlyContinue
        Write-Host "All bearing simulators stopped." -ForegroundColor Yellow
    }
}

function Run-ReplayPhmDlq {
    Write-Step "Replaying PHM DLQ back to raw topic"
    $pythonExe = Get-PythonExe
    $scriptPath = Join-Path $PSScriptRoot "scripts/replay_phm_dlq.py"
    & $pythonExe $scriptPath --bootstrap $KafkaBootstrap --from-beginning
}

function Run-TrainPhmWithRetry([string]$PythonExe, [int]$MaxRetries = 3) {
    $oldOneDnn = $env:TF_ENABLE_ONEDNN_OPTS
    $oldCuda = $env:CUDA_VISIBLE_DEVICES
    $oldAwsKey = $env:AWS_ACCESS_KEY_ID
    $oldAwsSecret = $env:AWS_SECRET_ACCESS_KEY
    $oldS3Endpoint = $env:MLFLOW_S3_ENDPOINT_URL
    $env:TF_ENABLE_ONEDNN_OPTS = "0"
    $env:CUDA_VISIBLE_DEVICES = "-1"
    $env:AWS_ACCESS_KEY_ID = "minioadmin"
    $env:AWS_SECRET_ACCESS_KEY = "minioadmin123"
    $env:MLFLOW_S3_ENDPOINT_URL = "http://localhost:9000"

    try {
        for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
            Write-Host "PHM training attempt $attempt/$MaxRetries..." -ForegroundColor Yellow
            & $PythonExe "scripts/train_phm_model.py"
            if ($LASTEXITCODE -eq 0) {
                Write-Host "PHM training completed successfully." -ForegroundColor Green
                return
            }
            if ($attempt -lt $MaxRetries) {
                Write-Host "PHM training failed (attempt $attempt). Retrying..." -ForegroundColor Yellow
            }
        }
    }
    finally {
        if ($null -eq $oldOneDnn) { Remove-Item Env:\TF_ENABLE_ONEDNN_OPTS -ErrorAction SilentlyContinue }
        else { $env:TF_ENABLE_ONEDNN_OPTS = $oldOneDnn }

        if ($null -eq $oldCuda) { Remove-Item Env:\CUDA_VISIBLE_DEVICES -ErrorAction SilentlyContinue }
        else { $env:CUDA_VISIBLE_DEVICES = $oldCuda }

        if ($null -eq $oldAwsKey) { Remove-Item Env:\AWS_ACCESS_KEY_ID -ErrorAction SilentlyContinue }
        else { $env:AWS_ACCESS_KEY_ID = $oldAwsKey }

        if ($null -eq $oldAwsSecret) { Remove-Item Env:\AWS_SECRET_ACCESS_KEY -ErrorAction SilentlyContinue }
        else { $env:AWS_SECRET_ACCESS_KEY = $oldAwsSecret }

        if ($null -eq $oldS3Endpoint) { Remove-Item Env:\MLFLOW_S3_ENDPOINT_URL -ErrorAction SilentlyContinue }
        else { $env:MLFLOW_S3_ENDPOINT_URL = $oldS3Endpoint }
    }

    throw "Train PHM failed"
}

function Consume-Raw {
    Write-Step "Consuming Kafka RAW topic ($RawTopic)"
    Ensure-Command "docker"
    Ensure-KafkaTopics

    Invoke-Checked {
        docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
            --topic $RawTopic `
            --from-beginning `
            --property print.key=true `
            --bootstrap-server kafka:9092
    } "Kafka raw consumer failed"
}

function Consume-Dlq {
    Write-Step "Consuming Kafka DLQ topic ($DlqTopic)"
    Ensure-Command "docker"
    Ensure-KafkaTopics

    Invoke-Checked {
        docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
            --topic $DlqTopic `
            --from-beginning `
            --bootstrap-server kafka:9092
    } "Kafka DLQ consumer failed"
}

function Compose-Status {
    Write-Step "Docker compose status"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile ingest --profile bronze --profile ops --profile train --profile dashboard ps } "Docker compose status failed"
}

function Compose-DownAll {
    Write-Step "Stopping all stages"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile ingest --profile bronze --profile ops --profile train --profile dashboard down } "Docker compose down failed"
}

function Show-Logs {
    Write-Step "Showing service logs ($Service)"
    Ensure-Command "docker"

    if ($Follow) {
        Invoke-Checked { docker compose --profile core --profile ingest --profile bronze --profile ops --profile train --profile dashboard logs -f --tail $Tail $Service } "Failed to follow service logs"
    }
    else {
        Invoke-Checked { docker compose --profile core --profile ingest --profile bronze --profile ops --profile train --profile dashboard logs --tail $Tail $Service } "Failed to show service logs"
    }
}

function Check-Health {
    Write-Step "Health checks"
    Ensure-Command "docker"

    $services = @("emqx", "kafka", "mqtt-kafka-bridge", "minio", "bronze-telemetry", "silver-gold-inference-alert", "dashboard-db", "gold-sync", "superset", "grafana")
    foreach ($svc in $services) {
        $id = docker compose @ComposeProfileAll ps -a -q $svc 2>$null
        if (-not $id) {
            Write-Host "$svc : not created" -ForegroundColor DarkYellow
            continue
        }

        $running = docker inspect --format "{{.State.Running}}" $id 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "$svc : unknown" -ForegroundColor Yellow
            continue
        }

        if ($running -eq "true") {
            Write-Host "$svc : running" -ForegroundColor Green
        }
        else {
            Write-Host "$svc : stopped" -ForegroundColor DarkYellow
        }
    }
}

function Clean-PhmData {
    Write-Step "Cleaning PHM Data from MinIO, Kafka and PostgreSQL"
    
    # 1. Clean MinIO Data
    Write-Host "Cleaning up MinIO Data & Checkpoints for PHM..." -ForegroundColor Yellow
    $minioPaths = "local/checkpoints/bronze/phm_raw/ " +
                  "local/checkpoints/bronze/phm_dlq/ " +
                  "local/checkpoints/silver/phm_stream_clean/ " +
                  "local/lakehouse/bronze/phm_raw/ " +
                  "local/lakehouse/bronze/phm_dlq/ " +
                  "local/lakehouse/silver/phm_stream_clean/ " +
                  "local/lakehouse/gold/prediction_current_phm/ " +
                  "local/lakehouse/gold/alert_current_phm/ " +
                  "local/lakehouse/gold/pipeline_quality_phm/ " +
                  "local/lakehouse/gold/prediction_history_phm/ " +
                  "local/lakehouse/gold/alert_history_phm/"

    # Ensure minio is running before executing commands
    Invoke-Checked { docker compose --profile core --profile bronze-phm up -d minio } "Failed to start MinIO for reset"
    $mcCommand = "mc alias set local http://localhost:9000 minioadmin minioadmin123 >/dev/null 2>&1 && mc rm -r --force $minioPaths >/dev/null 2>&1"
    docker compose exec -T minio bash -c $mcCommand
    
    # 2. PostgreSQL
    $dbContainerId = docker compose ps -q dashboard-db 2>$null
    if ($dbContainerId) {
        Write-Host "Truncating PHM tables in PostgreSQL..." -ForegroundColor Yellow
        $tablesToTruncate = "gold.gold_prediction_history_phm, gold.gold_prediction_current_phm, gold.gold_alert_history_phm, gold.gold_alert_current_phm"
        docker compose exec -T dashboard-db psql -U pdm -d pdm_dashboard -c "TRUNCATE TABLE $tablesToTruncate RESTART IDENTITY CASCADE;"
    } else {
        Write-Host "dashboard-db is not running. Skipping PostgreSQL truncate." -ForegroundColor DarkYellow
    }

    # 3. Kafka
    Write-Host "Deleting Kafka topics pdm.phm.raw and pdm.phm.raw.dlq..." -ForegroundColor Yellow
    docker compose --profile core exec -T kafka /opt/kafka/bin/kafka-topics.sh --delete --if-exists --topic pdm.phm.raw --bootstrap-server kafka:9092
    docker compose --profile core exec -T kafka /opt/kafka/bin/kafka-topics.sh --delete --if-exists --topic pdm.phm.raw.dlq --bootstrap-server kafka:9092

    Write-Host "PHM Reset Completed." -ForegroundColor Green
}

function Normalize-LegacyAction([string]$RequestedAction) {
    switch ($RequestedAction) {
        "up" { return "up-core" }
        "bridge" { return "up-ingest" }
        "bronze" { return "up-bronze" }
        "down" { return "down-all" }
        "all" { return "up-all" }
        default { return $RequestedAction }
    }
}

 $NormalizedAction = Normalize-LegacyAction -RequestedAction $Action

if ($Action -ne $NormalizedAction) {
    Write-Host "Legacy action '$Action' mapped to '$NormalizedAction'." -ForegroundColor Yellow
}

switch ($NormalizedAction) {
    "install" {
        Install-Dependencies
    }
    "split-train-stream" {
        Run-SplitTrainStream
    }
    "up-core" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Wait-EmqxHealthy
    }
    "down-core" {
        Compose-DownCore
    }
    "up-ingest" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Wait-EmqxHealthy
        Start-Ingest
        Wait-BridgeReady
    }
    "down-ingest" {
        Stop-Ingest
    }
    "up-bronze" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Start-Bronze
    }
    "down-bronze" {
        Stop-Bronze
    }
    "up-ops" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Start-Ops
    }
    "down-ops" {
        Stop-Ops
    }
    "up-phm" {
        Compose-UpCore
        Wait-KafkaReady
        
        # Reset PHM data before starting the pipelines
        Clean-PhmData
        
        Ensure-KafkaTopic -TopicName "pdm.phm.raw"
        Ensure-KafkaTopic -TopicName "pdm.phm.raw.dlq" -Partitions 1
        Invoke-Checked { docker compose --profile core --profile ingest-phm --profile bronze-phm --profile ops-phm up -d mqtt-kafka-bridge-phm bronze-telemetry-phm mlflow silver-gold-phm } "Failed to start PHM stage"
    }
    "down-phm" {
        Invoke-Checked { docker compose --profile ingest-phm --profile bronze-phm --profile ops-phm stop mqtt-kafka-bridge-phm bronze-telemetry-phm mlflow silver-gold-phm } "Failed to stop PHM stage"
    }
    "clean-phm" {
        Clean-PhmData
    }
    "train-phm" {
        $pythonExe = Get-PythonExe
        Invoke-Checked { docker compose --profile core --profile bronze up -d minio minio-init } "Start MinIO failed"
        Invoke-Checked { docker compose --profile train up -d mlflow } "Start MLflow failed"
        
        # Wait for MLflow to be healthy before training
        Write-Step "Waiting for MLflow to be ready..."
        $mlflowReady = $false
        for ($i = 0; $i -lt 30; $i++) {
            try {
                $response = Invoke-WebRequest -Uri "http://localhost:5000/health" -UseBasicParsing -ErrorAction SilentlyContinue
                if ($response.StatusCode -eq 200) {
                    $mlflowReady = $true
                    Write-Host "[OK] MLflow is ready" -ForegroundColor Green
                    break
                }
            }
            catch {
                # Not ready yet
            }
            if ($i -lt 29) {
                Write-Host "  Waiting... ($($i+1)/30)" -ForegroundColor Yellow
                Start-Sleep -Seconds 1
            }
        }
        
        if (-not $mlflowReady) {
            Write-Host "[WARN] MLflow health check failed, but proceeding with training (will use local mode if needed)" -ForegroundColor Yellow
        }
        
        Write-Step "Training PHM LSTM model"
        Run-TrainPhmWithRetry -PythonExe $pythonExe -MaxRetries 3
    }
    "train-nasa" {
        if (-not (Test-Path $RawStreamingCsv)) {
            Write-Step "Holdout CSV missing; running split-train-stream first"
            & $PSCommandPath -Action split-train-stream -TrainRatio $TrainRatio -SplitSeed $SplitSeed
        }
        $imageName = "pdm-ops-eval"
        if (-not (docker image inspect $imageName 2>$null)) {
            Write-Step "Building Docker image $imageName (tensorflow-cpu)"
            Invoke-Checked { docker build -f Dockerfile.ops -t $imageName . } "Docker build failed"
        }
        Write-Step "Training NASA FD001 LSTM (unit-level validation + holdout metrics)"
        Invoke-Checked {
            docker run --rm `
                -v "${PSScriptRoot}:/app" `
                -w /app `
                -e TF_ENABLE_ONEDNN_OPTS=0 `
                $imageName python scripts/train_nasa_model.py --force
        } "NASA LSTM training failed"
    }
    "up-dashboard" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Start-Bronze
        Start-Dashboard
    }
    "down-dashboard" {
        Stop-Dashboard
    }
    "refresh-superset" {
        Refresh-SupersetMeta
    }
    "build-train-silver-gold" {
        Build-TrainSilverGold
    }
    "down-train" {
        Stop-TrainStage
    }
    "up-all" {
        Install-Dependencies
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Wait-EmqxHealthy
        Start-Ingest
        Wait-BridgeReady
        Start-Bronze
        Start-Ops
    }
    "down-all" {
        Compose-DownAll
    }
    "replay" {
        Run-Replay
    }
    "replay-phm" {
        Run-ReplayPhm
    }
    "replay-phm-dlq" {
        Run-ReplayPhmDlq
    }
    "consume-raw" {
        Consume-Raw
    }
    "consume-dlq" {
        Consume-Dlq
    }
    "logs" {
        Show-Logs
    }
    "status" {
        Compose-Status
    }
    "health" {
        Check-Health
    }
}
