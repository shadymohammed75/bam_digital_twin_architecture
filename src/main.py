import argparse
import os
from pathlib import Path
import pandas as pd
from Builder import SubmodelBuilder
from db_writer import SQLiteStorage

def process_pipeline(template_path, csv_path, output_path, db_path):
    """Complete pipeline: CSV → Database → AAS Submodel"""
    try:
        # 1. Load and validate CSV
        print(f"[1/3] Loading CSV data from {csv_path}...")
        df = pd.read_csv(csv_path)
        print("Sample data:", df.head(2))

        # 2. Store in SQLite database
        print(f"[2/3] Storing in database {db_path}...")
        storage = SQLiteStorage(db_path=db_path)
        for sensor_id, group in df.groupby("sensor_id"):
            storage.append_records(sensor_id, group.to_dict("records"))
        storage.close()

        # 3. Generate AAS Submodel
        print(f"[3/3] Generating AAS Submodel at {output_path}...")
        builder = SubmodelBuilder(template_path)
        builder.process_csv(csv_path)
        builder._save_submodel(output_path)

        print("[SUCCESS] Pipeline completed successfully!")
    except Exception as e:
        print(f"[ERROR] Pipeline failed: {str(e)}")
        raise

def main():
    # Set up argument parsing with the exact Windows path
    parser = argparse.ArgumentParser(description='Process sensor data into DB and AAS Submodel')

    # Use os.path.normpath() for Windows paths
    default_template_path = os.path.normpath(
        r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\IDTA_02008-1-1_Template_TimeSeriesData.json"
    )

    parser.add_argument('--template',
                      default=default_template_path,
                      help='AAS template JSON path (default: %(default)s)')

    # Rest of the arguments
    parser.add_argument('--csv',
                      default=os.path.normpath(
                          r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\sample_timeseries_sleep10ms.csv"),
                      help='Input CSV path')
    parser.add_argument('--output',
                      default=os.path.normpath(
                          r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\TimeSeriesDataInstance.json"),
                      help='Output submodel JSON path')
    parser.add_argument('--db',
                      default=os.path.normpath(r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\sensors.db"),
                      help='SQLite database path')

    args = parser.parse_args()

    # Validate paths using Path from pathlib
    for path in [args.template, args.csv]:
        if not Path(path).exists():
            raise FileNotFoundError(f"File not found: {path}")

    # Run the pipeline
    process_pipeline(
        template_path=args.template,
        csv_path=args.csv,
        output_path=args.output,
        db_path=args.db
    )

if __name__ == "__main__":
    main()