"""Defines trends calculations for stations"""
import logging

import faust
from dataclasses import dataclass


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.stations", value_type=Station, key_type=int)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1, value_type=TransformedStation)

table = app.Table(
    "org.chicago.cta.stations.table.v1",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

@app.agent(topic)
async def transform_stations(stations):
    line = ""
    async for station in stations:
        if station.red == True:
            line = "red"
        elif station.blue == True:
            line = "blue"
        if station.green == True:
            line = "green"
        else:
            logger.info("There is no color for that line: {station.station_id}")

        table[station.station_id] = TransformedStation(
            station_id = station.station_id,
            station_name = station.station_name,
            order = station.order,
            line = line
        )
        #await out_topic.send(value=stations_transformed, key=station.station_id)

if __name__ == "__main__":
    app.main()
