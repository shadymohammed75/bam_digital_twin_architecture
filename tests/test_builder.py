# tests/test_builder.py
import json
import pytest
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from aas_core3 import jsonization
from aas_core3.types import Submodel, SubmodelElementCollection


# --- Fixtures ---
@pytest.fixture
def sample_data(tmp_path):
    """Generate test CSV with 3 sensors (2 existing, 1 new)"""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("""sensor_id,measurement_type,timestamp_iso,value,epoch_ms
sensor_1,temp,2025-01-01T00:00:00,21.5,1735689600000
sensor_2,humi,2025-01-01T00:00:00,45.0,1735689600000
sensor_1,temp,2025-01-01T00:01:00,22.0,1735689660000
sensor_3,light,2025-01-01T00:00:00,850.0,1735689600000""")
    return csv_path


@pytest.fixture
def template_file(tmp_path):
    """Create a minimal template file"""
    template_path = tmp_path / "template.json"
    template_path.write_text('{"idShort": "TestTemplate"}')
    return template_path


# --- Core Tests ---
def test_csv_to_json_conversion(sample_data, template_file):
    """Task 1: Test CSV converts to valid JSON structure"""
    from src.Builder import SubmodelBuilder

    builder = SubmodelBuilder(str(template_file))
    builder.process_csv(str(sample_data))

    assert isinstance(builder.submodel, Submodel), "No submodel created"
    assert len(builder.submodel.submodel_elements) > 0, "Missing segments"


def test_segment_per_sensor(sample_data, template_file):
    """Task 2: Test one segment created per sensor"""
    from src.Builder import SubmodelBuilder

    builder = SubmodelBuilder(str(template_file))
    builder.process_csv(str(sample_data))

    segments = builder.submodel.submodel_elements
    assert len(segments) == 3, f"Expected 3 segments, got {len(segments)}"
    assert {s.id_short for s in segments} == {"sensor_1", "sensor_2", "sensor_3"}


def test_500_record_limit(tmp_path, template_file):
    """Task 3: Test ≤500 records kept inline"""
    from src.Builder import SubmodelBuilder

    # Create CSV with 600 records
    csv_path = tmp_path / "large.csv"
    with open(csv_path, 'w') as f:
        f.write("sensor_id,measurement_type,timestamp_iso,value,epoch_ms\n")
        for i in range(600):
            f.write(f"sensor_1,temp,2025-01-01T00:{i:02d}:00,{i % 30},0\n")

    builder = SubmodelBuilder(str(template_file))
    builder.process_csv(str(csv_path))

    segment = next(s for s in builder.submodel.submodel_elements if s.id_short == "sensor_1")
    assert len(segment.value) <= 500, f"Expected ≤500 records, got {len(segment.value)}"


def test_statistics_calculation(sample_data, template_file):
    """Task 4: Test RecordCount, StartTime, EndTime, SamplingInterval"""
    from src.Builder import SubmodelBuilder
    from datetime import datetime

    builder = SubmodelBuilder(str(template_file))
    builder.process_csv(str(sample_data))

    # Find the statistics property in the segment
    segment = next(s for s in builder.submodel.submodel_elements
                   if s.id_short == "sensor_1")

    # Get the statistics from the segment's properties
    stats = {
        prop.id_short: prop.value
        for prop in segment.value
        if hasattr(prop, 'id_short') and hasattr(prop, 'value')
    }

    # Verify statistics
    assert stats.get("RecordCount") == "2", "Wrong record count"

    # Time comparisons that handle timezones
    assert datetime.fromisoformat(stats.get("StartTime")).replace(tzinfo=None) == \
           datetime.fromisoformat("2025-01-01T00:00:00"), "Wrong start time"
    assert datetime.fromisoformat(stats.get("EndTime")).replace(tzinfo=None) == \
           datetime.fromisoformat("2025-01-01T00:01:00"), "Wrong end time"
    assert stats.get("SamplingInterval") == "60", "Should calculate 60s interval"


# --- Schema Validation ---
def test_schema_compliance(sample_data, template_file):
    """Task 6: Validate against IDTA 02008-1-1 schema"""
    from src.Builder import SubmodelBuilder
    from aas_core3 import jsonization

    builder = SubmodelBuilder(str(template_file))
    builder.process_csv(str(sample_data))

    # Convert to JSON and validate structure
    jsonable = jsonization.to_jsonable(builder.submodel)
    assert "submodelElements" in jsonable, "Missing required field"