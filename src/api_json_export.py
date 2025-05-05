from fastapi import FastAPI, HTTPException
from pathlib import Path
import json

app = FastAPI(
    title="AAS JSON Export API",
    version="1.0",
    description="API for accessing Asset Administration Shell Time Series Data"
)

# Define the path to use already generated AAS submodel JSON
AAS_JSON_PATH = Path(r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\TimeSeriesDataInstance.json")

# Define the path to your JSON file
AAS_JSON_PATH = Path(r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\TimeSeriesAAS.json")


@app.get("/api/v1/aas/{asset_id}/submodels/time-series",
         summary="Get Time Series Submodel",
         description="Returns the complete AAS submodel in JSON format")
def get_submodel(asset_id: str):
    if not AAS_JSON_PATH.exists():
        raise HTTPException(status_code=404, detail="AAS Submodel JSON file not found.")

    with open(AAS_JSON_PATH, "r", encoding="utf-8") as file:
        submodel = json.load(file)

    return {
        "asset_id": asset_id,
        "submodel": submodel
    }
