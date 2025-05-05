import argparse
import os
from pathlib import Path
import pandas as pd
from Builder import SubmodelBuilder
from db_writer import SQLiteStorage


def process_pipeline(template_path, csv_path, output_path, db_path):
    """Complete pipeline: CSV → Database → AAS"""
    try:
        # 1. Load and validate CSV
        print(f"[1/3] Loading CSV data from {csv_path}...")
        df = pd.read_csv(csv_path)
        print("Sample data:\n", df.head(2))

        # 2. Store in SQLite database
        print(f"[2/3] Storing in database {db_path}...")
        storage = SQLiteStorage(db_path=db_path)
        for sensor_id, group in df.groupby("sensor_id"):
            storage.append_records(sensor_id, group.to_dict("records"))
        storage.close()

        # 3. Generate AAS
        print(f"[3/3] Generating AAS at {output_path}...")
        builder = SubmodelBuilder(template_path)
        submodel_elements = builder.process_csv(csv_path)
        builder.build_full_aas(submodel_elements, output_path)

        print(f"[SUCCESS] Pipeline completed! Output saved to {output_path}")
    except Exception as e:
        print(f"[ERROR] Pipeline failed: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description='Process sensor data into DB and AAS')

    default_paths = {
        'template': r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\IDTA_02008-1-1_Template_TimeSeriesData.json",
        'csv': r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\sample_timeseries_sleep10ms.csv",
        'output': r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\TimeSeriesAAS.json",
        'db': r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\sensors.db"
    }

    for arg, path in default_paths.items():
        parser.add_argument(f'--{arg}',
                            default=os.path.normpath(path),
                            help=f'{arg} path (default: %(default)s)')

    args = parser.parse_args()

    # Validate paths
    for path in [args.template, args.csv]:
        if not Path(path).exists():
            raise FileNotFoundError(f"File not found: {path}")

    # Ensure output directory exists
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    process_pipeline(
        template_path=args.template,
        csv_path=args.csv,
        output_path=args.output,
        db_path=args.db
    )


if __name__ == "__main__":
    main()