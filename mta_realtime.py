import os
import requests
import logging
import pandas as pd
import time
import json
from mta_reference import MTAReference
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s]  %(message)s",
    handlers=[logging.StreamHandler()],
)

mta_feeds = (
    # "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",  # A,C,E,Sr
    # "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",  # B,D,F,M,Sf
    # "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",  # G
    # "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",  # J,Z
    # "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",  # N,Q,R,W
    # "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",  # L
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",  # 1-7, S
    # "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",  # SIR
)

load_dotenv()
bootstrap_servers = os.getenv("REDPANDA_SERVERS")
topic = os.getenv("REDPANDA_TOPIC")

# Kafka producer
producer = Producer(
    {
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 10,
        "request.timeout.ms": 1000,
    }
)
logging.info(f"Kafka bootstrap servers: {bootstrap_servers}")

# Route reference data
ref = MTAReference()
routes_df = ref.get_routes()

# Stop reference data
stops_df = ref.get_stops()


def get_route_name(route_id: str) -> str:
    """Returns route name for route id"""
    name: str = None
    try:
        name = routes_df.loc[route_id].iloc[0]
    except KeyError:
        logging.debug(f"Unknown route: {route_id}")
    return name


def get_stop_name(stop_id: str) -> str:
    """Returns stop name for stop id"""
    name: str = None
    try:
        name = stops_df.loc[stop_id].iloc[0]
    except KeyError:
        logging.debug(f"Unknown stop: {stop_id}")
    return name


def get_direction(stop_id: str) -> str:
    """Returns direction of travel (North or South) for stop id."""
    direction: str = ""
    if stop_id[-1].upper() == "N":
        direction = "North"
    elif stop_id[-1].upper() == "S":
        direction = "South"
    return direction


def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")


def fetch_updates(url: str, routes: Optional[list[str]] = None):
    """Parse MTA GTFS realtime feed and send messages to Kafka.
    Optionally filter by route ids."""

    logging.info(f"Fetching: {url}")
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url)
    feed.ParseFromString(response.content)

    count = 0
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            for stop in trip_update.stop_time_update:
                # Include latest updates within 2 minute window
                arrived = datetime.fromtimestamp(stop.arrival.time)
                tmin = datetime.now() - timedelta(minutes=5)
                tmax = datetime.now() + timedelta(minutes=5)
                if (arrived < tmin) or (arrived > tmax):
                    continue
                if routes and trip_update.trip.route_id not in routes:
                    continue
                route_name = get_route_name(trip_update.trip.route_id)
                if route_name is None:
                    continue
                stop_name = get_stop_name(stop.stop_id)
                if stop_name is None:
                    continue
                stop_event = {
                    "trip_id": trip_update.trip.trip_id,
                    "route_id": trip_update.trip.route_id,
                    "route_name": route_name,
                    "stop_id": stop.stop_id,
                    "stop_name": stop_name,
                    "stop_direction": get_direction(stop.stop_id),
                    "arrival": datetime.fromtimestamp(stop.arrival.time).strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    ),
                    "departure": datetime.fromtimestamp(stop.departure.time).strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    ),
                }
                producer.produce(
                    topic,
                    key=str(trip_update.trip.route_id).encode("utf-8"),
                    value=json.dumps(stop_event).encode("utf-8"),
                    callback=delivery_report,
                )
                count += 1
    short = url.rsplit("/", 1)[-1]
    logging.info(f'Sent {count} trip updates to Kafka for feed "{short}"')


if __name__ == "__main__":
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    admin.delete_topics([topic])
    result = admin.create_topics(
        [
            NewTopic(topic, num_partitions=1, replication_factor=1),
        ],
    )
    for t, f in result.items():
        try:
            f.result()
            logging.info(f'Topic "{t}" created')
        except Exception as e:
            logging.error(f'Failed to create topic "{t}": {e}')

    try:
        while True:
            producer.poll(0)
            futures = []
            with ThreadPoolExecutor() as pool:
                for url in mta_feeds:
                    futures.append(pool.submit(fetch_updates, url, ["1"]))
                for f in as_completed(futures):
                    f.result()
            producer.flush()
            time.sleep(30)
    except KeyboardInterrupt:
        print("Stopping...")
