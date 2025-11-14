"""
Simple Prefect Flow Example

This is a starter example to learn Prefect basics.
It demonstrates:
- Tasks with @task decorator
- Flows with @flow decorator
- Task dependencies
- Parameters and return values
- Basic logging
"""

from prefect import flow, task
from datetime import datetime
import time


@task
def extract_data(source: str) -> dict:
    """
    Simulates data extraction from a source.

    Args:
        source: Name of the data source

    Returns:
        Dictionary with sample data
    """
    print(f"Extracting data from {source}...")
    time.sleep(1)  # Simulate API call delay

    # Simulated data
    data = {
        "source": source,
        "timestamp": datetime.now().isoformat(),
        "records": [
            {"id": 1, "value": 100.5},
            {"id": 2, "value": 250.75},
            {"id": 3, "value": 175.25},
        ]
    }

    print(f"Extracted {len(data['records'])} records from {source}")
    return data


@task
def transform_data(data: dict) -> dict:
    """
    Simulates data transformation.

    Args:
        data: Raw data dictionary

    Returns:
        Transformed data dictionary
    """
    print("Transforming data...")
    time.sleep(0.5)

    # Simple transformation: add calculated field
    for record in data["records"]:
        record["value_doubled"] = record["value"] * 2

    data["transformed_at"] = datetime.now().isoformat()
    print(f"Transformed {len(data['records'])} records")
    return data


@task
def load_data(data: dict, destination: str) -> str:
    """
    Simulates loading data to a destination.

    Args:
        data: Transformed data dictionary
        destination: Name of the destination

    Returns:
        Status message
    """
    print(f"Loading data to {destination}...")
    time.sleep(0.5)

    # Simulate writing to destination
    status = f"Successfully loaded {len(data['records'])} records to {destination}"
    print(status)
    return status


@flow(name="Simple ETL Flow")
def simple_etl_flow(source: str = "API", destination: str = "Database"):
    """
    A simple ETL flow that demonstrates Prefect basics.

    Args:
        source: Data source name
        destination: Data destination name
    """
    print(f"\n{'='*50}")
    print(f"Starting ETL Flow: {source} -> {destination}")
    print(f"{'='*50}\n")

    # Extract
    raw_data = extract_data(source)

    # Transform
    transformed_data = transform_data(raw_data)

    # Load
    result = load_data(transformed_data, destination)

    print(f"\n{'='*50}")
    print(f"Flow completed: {result}")
    print(f"{'='*50}\n")

    return result


if __name__ == "__main__":
    # Run the flow
    simple_etl_flow(source="Tiingo API", destination="AWS S3")
