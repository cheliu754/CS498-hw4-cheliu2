import csv
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()
URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")
CSV_PATH = "taxi_trips_clean.csv"


def load_row(tx, row):
    tx.run(
        """
        MERGE (d:Driver {driver_id: $driver_id})
        MERGE (c:Company {name: $company})
        MERGE (a:Area {area_id: $dropoff_area})
        MERGE (d)-[:WORKS_FOR]->(c)
        CREATE (d)-[:TRIP {
            trip_id: $trip_id,
            fare: $fare,
            trip_seconds: $trip_seconds
        }]->(a)
        """,
        trip_id=row["trip_id"],
        driver_id=row["driver_id"],
        company=row["company"],
        dropoff_area=int(row["dropoff_area"]),
        fare=float(row["fare"]),
        trip_seconds=int(row["trip_seconds"]),
    )

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

with driver.session() as session:
    with open(CSV_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            session.execute_write(load_row, row)

driver.close()
print("Graph loading completed.")
