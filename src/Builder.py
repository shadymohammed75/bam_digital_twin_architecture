import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Any
import pandas as pd
import numpy as np
import json
from aas_core3 import types
from aas_core3 import jsonization
from aas_core3.types import DataTypeDefXSD
from src.db_writer import SQLiteStorage
MAX_INLINE_RECORDS = 500


class SubmodelBuilder:
    def __init__(self, template_path: str):
        self.template = self._load_template(template_path)
        self.submodel = self._initialize_submodel()
        self.external_storage = SQLiteStorage()
        self.segment_map: Dict[str, Any] = {}

    @staticmethod
    def _load_template(path: str) -> Dict[str, Any]:
        """Load and return the JSON template from file."""
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _initialize_submodel(self) -> types.Submodel:
        """Initialize the AAS Submodel from template."""
        sm_dict = self.template
        return types.Submodel(
            id=sm_dict.get("id", f"urn:uuid:{uuid.uuid4()}"),
            id_short=sm_dict.get("idShort", "UnnamedSubmodel"),
            semantic_id=self._parse_reference(sm_dict.get("semanticId")),
            kind=types.ModellingKind[sm_dict.get("kind", "Instance").upper()],
            submodel_elements=[]
        )

    @staticmethod
    def _parse_reference(ref_dict: Dict[str, Any]) -> types.Reference | None:
        """Parse AAS reference from dictionary."""
        if ref_dict is None:
            return None
        keys = ref_dict.get("keys", [])
        return types.Reference(
            type=types.KeyTypes.REFERENCE,
            keys=[types.Key(type=types.KeyTypes[k["type"]], value=k["value"]) for k in keys]
        )

    def process_csv(self, csv_path: str) -> None:
        """Process CSV file and generate AAS submodel."""
        df = pd.read_csv(csv_path)
        for sensor_id, group in df.groupby("sensor_id"):
            self._process_sensor_data(sensor_id, group)
        self._save_submodel("data/TimeSeriesDataInstance.json")

    def _process_sensor_data(self, sensor_id: str, data: pd.DataFrame) -> None:
        """Process data for a single sensor."""
        segment = self.segment_map.get(sensor_id)
        if not segment:
            segment = self._create_new_segment(sensor_id)
            self.submodel.submodel_elements.append(segment)
            self.segment_map[sensor_id] = segment

        timestamps = data["epoch_ms"].sort_values().to_numpy()
        start_time = datetime.fromtimestamp(timestamps[0] / 1000, tz=timezone.utc).isoformat()
        end_time = datetime.fromtimestamp(timestamps[-1] / 1000, tz=timezone.utc).isoformat()
        sampling_interval = int(np.median(np.diff(timestamps)) / 1000) if len(timestamps) > 1 else 0

        inline_data = data.sort_values("epoch_ms").head(MAX_INLINE_RECORDS)
        records = []
        for _, row in inline_data.iterrows():
            record = types.SubmodelElementCollection(
                id_short="Record",
                value=[
                    types.Property(
                        id_short="Time",
                        value=row["timestamp_iso"],
                        value_type=DataTypeDefXSD.DATE_TIME
                    ),
                    types.Property(
                        id_short="Value",
                        value=str(row["value"]),
                        value_type=DataTypeDefXSD.DOUBLE
                    )
                ]
            )
            records.append(record)

        segment.value = [
            types.Property(
                id_short="Name",
                value=sensor_id,
                value_type=DataTypeDefXSD.STRING
            ),
            types.Property(
                id_short="Description",
                value=data["measurement_type"].iloc[0],
                value_type=DataTypeDefXSD.STRING
            ),
            types.Property(
                id_short="RecordCount",
                value=str(len(timestamps)),
                value_type=DataTypeDefXSD.INT
            ),
            types.Property(
                id_short="StartTime",
                value=start_time,
                value_type=DataTypeDefXSD.DATE_TIME
            ),
            types.Property(
                id_short="EndTime",
                value=end_time,
                value_type=DataTypeDefXSD.DATE_TIME
            ),
            types.Property(
                id_short="SamplingInterval",
                value=str(sampling_interval),
                value_type=DataTypeDefXSD.INT
            ),
            types.SubmodelElementCollection(
                id_short="Records",
                value=records
            )
        ]

        self.external_storage.append_records(sensor_id, data.to_dict('records'))

    @staticmethod
    def _create_new_segment(sensor_id: str) -> types.SubmodelElementCollection:
        """Create a new segment for a sensor."""
        return types.SubmodelElementCollection(id_short=sensor_id, value=[])

    def _save_submodel(self, output_path: str) -> None:
        """Save the submodel to JSON file."""
        jsonable = jsonization.to_jsonable(self.submodel)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(jsonable, f, indent=2)