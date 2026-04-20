from fastapi import FastAPI, Query
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

app = FastAPI()
driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))


def run_read_query(query: str, **params):
    with driver.session() as session:
        result = session.run(query, **params)
        return [record.data() for record in result]


@app.get("/graph-summary")
def graph_summary():
    query = """
    MATCH (d:Driver)
    WITH count(d) AS driver_count
    MATCH (c:Company)
    WITH driver_count, count(c) AS company_count
    MATCH (a:Area)
    WITH driver_count, company_count, count(a) AS area_count
    MATCH ()-[t:TRIP]->()
    RETURN driver_count, company_count, area_count, count(t) AS trip_count
    """
    rows = run_read_query(query)
    return rows[0]


@app.get("/top-companies")
def top_companies(n: int = Query(...)):
    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[:TRIP]->(:Area)
    RETURN c.name AS name, count(*) AS trip_count
    ORDER BY trip_count DESC
    LIMIT $n
    """
    rows = run_read_query(query, n=n)
    return {"companies": rows}


@app.get("/high-fare-trips")
def high_fare_trips(area_id: int = Query(...), min_fare: float = Query(...)):
    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
    WHERE t.fare > $min_fare
    RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
    ORDER BY t.fare DESC
    """
    rows = run_read_query(query, area_id=area_id, min_fare=min_fare)
    return {"trips": rows}


@app.get("/co-area-drivers")
def co_area_drivers(driver_id: str = Query(...)):
    query = """
    MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
    WHERE d1.driver_id <> d2.driver_id
    RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
    ORDER BY shared_areas DESC
    """
    rows = run_read_query(query, driver_id=driver_id)
    return {"co_area_drivers": rows}


@app.get("/avg-fare-by-company")
def avg_fare_by_company():
    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[t:TRIP]->(:Area)
    RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
    ORDER BY avg_fare DESC
    """
    rows = run_read_query(query)
    return {"companies": rows}