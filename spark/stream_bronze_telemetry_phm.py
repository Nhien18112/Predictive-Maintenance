import logging
import os
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, DoubleType, LongType

LOGGER = logging.getLogger("stream_bronze_telemetry_phm")

RAW_PAYLOAD_SCHEMA = StructType([
    StructField("unit_nr", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("time_cycles", LongType(), True),
    StructField("rms_x", DoubleType(), True),
    StructField("rms_y", DoubleType(), True),
    StructField("kurt_x", DoubleType(), True),
    StructField("kurt_y", DoubleType(), True),
    StructField("p2p_x", DoubleType(), True),
    StructField("p2p_y", DoubleType(), True),
    StructField("crest_x", DoubleType(), True),
    StructField("crest_y", DoubleType(), True),
    StructField("skew_x", DoubleType(), True),
    StructField("skew_y", DoubleType(), True),
    StructField("spec_entropy_x", DoubleType(), True),
    StructField("spec_entropy_y", DoubleType(), True),
    StructField("band_energy_x", DoubleType(), True),
    StructField("band_energy_y", DoubleType(), True),
    StructField("peak_freq_x", DoubleType(), True),
    StructField("peak_freq_y", DoubleType(), True),
    StructField("fft_x_1", DoubleType(), True),
    StructField("fft_x_2", DoubleType(), True),
    StructField("fft_x_3", DoubleType(), True),
    StructField("fft_x_4", DoubleType(), True),
    StructField("fft_x_5", DoubleType(), True),
    StructField("fft_y_1", DoubleType(), True),
    StructField("fft_y_2", DoubleType(), True),
    StructField("fft_y_3", DoubleType(), True),
    StructField("fft_y_4", DoubleType(), True),
    StructField("fft_y_5", DoubleType(), True),
    StructField("source_type", StringType(), True),
    StructField("source_file", StringType(), True)
])

DLQ_ENVELOPE_SCHEMA = StructType([
    StructField("error_type", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("received_at", StringType(), True),
    StructField("raw_payload", StringType(), True)
])

@dataclass(frozen=True)
class JobConfig:
    kafka_bootstrap_servers: str
    raw_topic: str
    dlq_topic: str
    starting_offsets: str
    fail_on_data_loss: str
    max_offsets_per_trigger: str
    trigger_interval: str
    bronze_raw_path: str
    bronze_dlq_path: str
    checkpoint_raw_path: str
    checkpoint_dlq_path: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str

    @staticmethod
    def from_env() -> "JobConfig":
        return JobConfig(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            raw_topic=os.getenv("BRONZE_RAW_TOPIC", "pdm.phm.raw"),
            dlq_topic=os.getenv("BRONZE_DLQ_TOPIC", "pdm.phm.raw.dlq"),
            starting_offsets=os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
            fail_on_data_loss=os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "false"),
            max_offsets_per_trigger=os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000"),
            trigger_interval=os.getenv("BRONZE_TRIGGER_INTERVAL", "10 seconds"),
            bronze_raw_path=os.getenv("BRONZE_RAW_DELTA_PATH", "s3a://lakehouse/bronze/phm_raw/"),
            bronze_dlq_path=os.getenv("BRONZE_DLQ_DELTA_PATH", "s3a://lakehouse/bronze/phm_dlq/"),
            checkpoint_raw_path=os.getenv("BRONZE_RAW_CHECKPOINT_PATH", "s3a://checkpoints/bronze/phm_raw/"),
            checkpoint_dlq_path=os.getenv("BRONZE_DLQ_CHECKPOINT_PATH", "s3a://checkpoints/bronze/phm_dlq/"),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        )

def create_spark_session(cfg: JobConfig) -> SparkSession:
    spark = (
        SparkSession.builder.appName("StreamBronzeTelemetryPHM")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark

def read_kafka_stream(spark: SparkSession, cfg: JobConfig, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", cfg.starting_offsets)
        .option("failOnDataLoss", cfg.fail_on_data_loss)
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("kafka_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").cast("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("spark_ingest_timestamp"),
        )
    )

def build_raw_bronze_df(df_kafka_raw: DataFrame) -> DataFrame:
    payload_col = F.from_json(F.col("kafka_value"), RAW_PAYLOAD_SCHEMA)
    return (
        df_kafka_raw.withColumn("payload", payload_col)
        .withColumn("payload_parse_ok", F.col("payload").isNotNull())
        .withColumn("raw_payload", F.col("kafka_value"))
        .select(
            "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
            "spark_ingest_timestamp", "kafka_key", "raw_payload", "payload_parse_ok",
            F.col("payload.unit_nr").alias("unit_nr"),
            F.col("payload.run_id").alias("run_id"),
            F.col("payload.event_time").alias("event_time"),
            F.col("payload.time_cycles").alias("time_cycles"),
            F.col("payload.rms_x").alias("rms_x"),
            F.col("payload.rms_y").alias("rms_y"),
            F.col("payload.kurt_x").alias("kurt_x"),
            F.col("payload.kurt_y").alias("kurt_y"),
            F.col("payload.p2p_x").alias("p2p_x"),
            F.col("payload.p2p_y").alias("p2p_y"),
            F.col("payload.crest_x").alias("crest_x"),
            F.col("payload.crest_y").alias("crest_y"),
            F.col("payload.skew_x").alias("skew_x"),
            F.col("payload.skew_y").alias("skew_y"),
            F.col("payload.spec_entropy_x").alias("spec_entropy_x"),
            F.col("payload.spec_entropy_y").alias("spec_entropy_y"),
            F.col("payload.band_energy_x").alias("band_energy_x"),
            F.col("payload.band_energy_y").alias("band_energy_y"),
            F.col("payload.peak_freq_x").alias("peak_freq_x"),
            F.col("payload.peak_freq_y").alias("peak_freq_y"),
            F.col("payload.fft_x_1").alias("fft_x_1"),
            F.col("payload.fft_x_2").alias("fft_x_2"),
            F.col("payload.fft_x_3").alias("fft_x_3"),
            F.col("payload.fft_x_4").alias("fft_x_4"),
            F.col("payload.fft_x_5").alias("fft_x_5"),
            F.col("payload.fft_y_1").alias("fft_y_1"),
            F.col("payload.fft_y_2").alias("fft_y_2"),
            F.col("payload.fft_y_3").alias("fft_y_3"),
            F.col("payload.fft_y_4").alias("fft_y_4"),
            F.col("payload.fft_y_5").alias("fft_y_5"),
            F.col("payload.source_type").alias("source_type"),
            F.col("payload.source_file").alias("source_file")
        )
    )

def build_dlq_bronze_df(df_kafka_dlq: DataFrame) -> DataFrame:
    envelope_col = F.from_json(F.col("kafka_value"), DLQ_ENVELOPE_SCHEMA)
    return df_kafka_dlq.withColumn("dlq_envelope", envelope_col).select(
        "kafka_topic", "kafka_partition", "kafka_offset", 
        "kafka_timestamp", "spark_ingest_timestamp", "kafka_key",
        F.col("kafka_value").alias("dlq_envelope_raw"),
        F.col("dlq_envelope.error_type").alias("error_type"),
        F.col("dlq_envelope.error_message").alias("error_message"),
        F.col("dlq_envelope.raw_payload").alias("raw_payload"),
        F.col("dlq_envelope.received_at").alias("bridge_received_at")
    )

def start_delta_sink(df: DataFrame, output_path: str, checkpoint_path: str, trigger_interval: str, query_name: str):
    def _write_and_log(batch_df: DataFrame, batch_id: int) -> None:
        if not batch_df.take(1): return
        batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(output_path)
        LOGGER.info(f"[{query_name}] batch={batch_id} saved rows={batch_df.count()}")

    return (
        df.writeStream
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .queryName(query_name)
        .foreachBatch(_write_and_log)
        .start()
    )

def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    cfg = JobConfig.from_env()
    spark = create_spark_session(cfg)

    raw_kafka_df = read_kafka_stream(spark, cfg, cfg.raw_topic)
    dlq_kafka_df = read_kafka_stream(spark, cfg, cfg.dlq_topic)

    bronze_raw_df = build_raw_bronze_df(raw_kafka_df)
    bronze_dlq_df = build_dlq_bronze_df(dlq_kafka_df)

    raw_query = start_delta_sink(bronze_raw_df, cfg.bronze_raw_path, cfg.checkpoint_raw_path, cfg.trigger_interval, "bronze_telemetry_raw_phm")
    dlq_query = start_delta_sink(bronze_dlq_df, cfg.bronze_dlq_path, cfg.checkpoint_dlq_path, cfg.trigger_interval, "bronze_telemetry_dlq_phm")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
