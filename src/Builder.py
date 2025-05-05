import json
from pathlib import Path
import pandas as pd
from uuid import uuid4


class SubmodelBuilder:
    def __init__(self, template_path: str = None):
        """Initialize with optional template"""
        self.template = json.loads(Path(template_path).read_text()) if template_path else None
        self.sensor_descriptions = {
            "temperature": "Temperature sensor in degrees Celsius",
            "pressure": "Pressure sensor in kPa"
        }

    def process_csv(self, csv_path: str) -> list:
        """Convert CSV data into AAS SubmodelElements"""
        df = pd.read_csv(csv_path)
        submodel_elements = []

        for sensor_id, group in df.groupby("sensor_id"):
            # Get measurement type from first record
            measurement_type = group.iloc[0]["measurement_type"]

            records = []
            for _, row in group.iterrows():
                records.append({
                    "idShort": "Record",
                    "modelType": "SubmodelElementCollection",
                    "value": [
                        {
                            "idShort": "Time",
                            "valueType": "xs:dateTime",
                            "value": row["timestamp_iso"],
                            "modelType": "Property"
                        },
                        {
                            "idShort": "Value",
                            "valueType": "xs:double",
                            "value": str(row["value"]),
                            "modelType": "Property"
                        }
                    ]
                })

            submodel_elements.append({
                "idShort": sensor_id,
                "modelType": "SubmodelElementCollection",
                "value": [
                    {
                        "idShort": "Name",
                        "valueType": "xs:string",
                        "value": sensor_id,
                        "modelType": "Property"
                    },
                    {
                        "idShort": "Description",
                        "valueType": "xs:string",
                        "value": self.sensor_descriptions.get(measurement_type, measurement_type),
                        "modelType": "Property"
                    },
                    {
                        "idShort": "RecordCount",
                        "valueType": "xs:int",
                        "value": str(len(records)),
                        "modelType": "Property"
                    },
                    {
                        "idShort": "StartTime",
                        "valueType": "xs:dateTime",
                        "value": group.iloc[0]["timestamp_iso"],
                        "modelType": "Property"
                    },
                    {
                        "idShort": "EndTime",
                        "valueType": "xs:dateTime",
                        "value": group.iloc[-1]["timestamp_iso"],
                        "modelType": "Property"
                    },
                    {
                        "idShort": "SamplingInterval",
                        "valueType": "xs:int",
                        "value": "60",  # 60 seconds between samples
                        "modelType": "Property"
                    },
                    {
                        "idShort": "Records",
                        "modelType": "SubmodelElementCollection",
                        "value": records
                    }
                ]
            })

        return submodel_elements

    def build_full_aas(self, submodel_elements: list, output_path: str):
        """Generate AAS aligned with template structure"""
        aas_environment = {
            "assetAdministrationShells": [{
                "idShort": "TimeSeriesAAS",
                "id": "https://admin-shell.io/idta/aas/TimeSeries/1/1",
                "modelType": "AssetAdministrationShell",
                "assetInformation": {
                    "assetKind": "Type",
                    "globalAssetId": "https://admin-shell.io/idta/asset/TimeSeries/1/1",
                    "assetType": "Type"
                },
                "submodels": [{
                    "type": "ModelReference",
                    "keys": [{
                        "type": "Submodel",
                        "value": "https://admin-shell.io/idta/submodel/TimeSeries/1/1"
                    }]
                }]
            }],
            "submodels": [{
                "idShort": "TimeSeriesData",
                "id": "https://admin-shell.io/idta/submodel/TimeSeries/1/1",
                "modelType": "Submodel",
                "semanticId": {
                    "type": "ExternalReference",
                    "keys": [{
                        "type": "GlobalReference",
                        "value": "https://admin-shell.io/idta/SubmodelTemplate/TimeSeries/1/1"
                    }]
                },
                "submodelElements": submodel_elements
            }]
        }

        Path(output_path).write_text(json.dumps(aas_environment, indent=2))