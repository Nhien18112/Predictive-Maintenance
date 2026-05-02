import os
import glob
import time
import json
import argparse
import datetime as dt
import numpy as np
import scipy.stats
import paho.mqtt.client as mqtt
import pandas as pd

CSV_COLS = ["Hour", "Minute", "Second", "Microsecond", "Acc_x", "Acc_y"]

def parse_args():
    parser = argparse.ArgumentParser(description="Simulate Edge device for PHM 2012 Streaming")
    parser.add_argument("--bearing-folder", required=True, help="Path to Bearing folder (e.g. Data/.../Test_set/Bearing1_3)")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT port")
    parser.add_argument("--topic", default="factory/pdm/phm/raw", help="MQTT Topic")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between sending each file (seconds)")
    parser.add_argument("--run-id", default="", help="Run identifier to group a replay session")
    return parser.parse_args()

def calculate_features(df):
    x = df["Acc_x"].values
    y = df["Acc_y"].values
    rms_x = np.sqrt(np.mean(x**2))
    rms_y = np.sqrt(np.mean(y**2))
    
    # Calculate Frequency Domain Features via FFT
    fft_x = np.abs(np.fft.rfft(x))
    fft_y = np.abs(np.fft.rfft(y))
    fft_x_amps = sorted([float(v) for v in fft_x], reverse=True)
    fft_y_amps = sorted([float(v) for v in fft_y], reverse=True)
    while len(fft_x_amps) < 5: fft_x_amps.append(0.0)
    while len(fft_y_amps) < 5: fft_y_amps.append(0.0)

    def _spectral_entropy(fft_vals):
        power = np.square(fft_vals)
        total = np.sum(power)
        if total == 0:
            return 0.0
        p = power / total
        p = p[p > 0]
        return float(-np.sum(p * np.log(p)))

    def _band_energy(fft_vals, ratio=0.1):
        n = len(fft_vals)
        if n == 0:
            return 0.0
        k = max(1, int(n * ratio))
        return float(np.sum(np.square(fft_vals[1:k])))

    def _peak_freq(fft_vals):
        if len(fft_vals) <= 1:
            return 0.0
        peak_idx = int(np.argmax(fft_vals[1:]) + 1)
        return float(peak_idx)

    res = {
        "rms_x": float(rms_x), "rms_y": float(rms_y),
        "kurt_x": float(scipy.stats.kurtosis(x)), "kurt_y": float(scipy.stats.kurtosis(y)),
        "p2p_x": float(np.ptp(x)), "p2p_y": float(np.ptp(y)),
        "crest_x": float(np.max(np.abs(x)) / rms_x) if rms_x > 0 else 0,
        "crest_y": float(np.max(np.abs(y)) / rms_y) if rms_y > 0 else 0,
        "skew_x": float(scipy.stats.skew(x)), "skew_y": float(scipy.stats.skew(y)),
        "spec_entropy_x": _spectral_entropy(fft_x),
        "spec_entropy_y": _spectral_entropy(fft_y),
        "band_energy_x": _band_energy(fft_x),
        "band_energy_y": _band_energy(fft_y),
        "peak_freq_x": _peak_freq(fft_x),
        "peak_freq_y": _peak_freq(fft_y),
    }
    
    for i in range(5):
        res[f"fft_x_{i+1}"] = fft_x_amps[i]
        res[f"fft_y_{i+1}"] = fft_y_amps[i]
        
    return res

def main():
    args = parse_args()
    run_id = args.run_id.strip() or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    unit_nr = os.path.basename(os.path.normpath(args.bearing_folder))
    files = sorted(glob.glob(os.path.join(args.bearing_folder, "acc_*.csv")))
    
    if not files:
        print(f"No acc_*.csv files found in {args.bearing_folder}")
        return

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=f"phm_sim_{unit_nr}")
    print(f"Connecting to MQTT {args.broker}:{args.port}...")
    client.connect(args.broker, args.port, keepalive=60)
    client.loop_start()

    print(f"Starting replay for {unit_nr}. Total files: {len(files)} | run_id={run_id}")
    
    try:
        for idx, f in enumerate(files):
            df = pd.read_csv(f, header=None, names=CSV_COLS, sep=r';|,', engine='python')
            features = calculate_features(df)
            
            payload = {
                "unit_nr": unit_nr,
                "run_id": run_id,
                "event_time": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                "time_cycles": idx + 1,
                "source_type": "phm_2012_simulator",
                "source_file": os.path.basename(f)
            }
            payload.update(features) # Add extracted features
            
            client.publish(args.topic, json.dumps(payload), qos=1)
            print(f"[{idx+1}/{len(files)}] Published {unit_nr} -> {args.topic}")
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        client.loop_stop()
        client.disconnect()
        print("Disconnected")

if __name__ == "__main__":
    main()
