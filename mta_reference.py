# To download latest timetable reference data:
#   rm -rf ../gtfs-reference/*
#   curl -O http://web.mta.info/developers/data/nyct/subway/google_transit.zip
#   unzip ./google_transit.zip -d ../gtfs-reference/
#   rm -f ./google_transit.zip

import os
import logging
import pandas as pd
from typing import Optional
from pandas import DataFrame, Series
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s]  %(message)s",
    handlers=[logging.StreamHandler()],
)


class MTAReference:
    base_dir = "./gtfs-reference"

    def __init__(self):
        self.routes = MTAReference.load_routes()
        self.trips = MTAReference.load_trips()
        self.stops = MTAReference.load_stops()
        self.stop_times = MTAReference.load_stop_times()

        self.journeys = self.stop_times.merge(self.trips, on="trip_id", how="left")
        self.journeys = self.journeys.merge(self.stops, on="stop_id", how="left")
        self.journeys = self.journeys.merge(self.routes, on="route_id", how="left")
        self.journeys = self.journeys.sort_values(by=["trip_id", "stop_sequence"])
        self.filter_journeys()
        self.enrich_journeys()

    def get_journeys(self) -> DataFrame:
        return self.journeys

    def get_routes(self) -> DataFrame:
        return self.routes

    def get_trips(self) -> DataFrame:
        return self.trips

    def get_stops(self) -> DataFrame:
        return self.stops

    def get_stop_times(self) -> DataFrame:
        return self.stop_times

    def filter_journeys(self):
        """Filter journeys to include route "1" trains for the next three hours."""

        self.journeys = self.journeys[self.journeys["route_id"] == "1"]
        self.journeys["arrival_time"] = pd.to_datetime(
            self.journeys["arrival_time"], format="%H:%M:%S", errors="coerce"
        )
        self.journeys["departure_time"] = pd.to_datetime(
            self.journeys["departure_time"], format="%H:%M:%S", errors="coerce"
        )
        self.journeys["arrival_time"] = self.journeys["arrival_time"].dt.time
        self.journeys["departure_time"] = self.journeys["departure_time"].dt.time
        start = datetime.now().strftime("%H:%M:%S")
        end = (datetime.now() + timedelta(hours=3)).strftime("%H:%M:%S")
        start_mask = self.journeys["arrival_time"] >= pd.Timestamp(start).time()
        end_mask = self.journeys["arrival_time"] <= pd.Timestamp(end).time()
        self.journeys = self.journeys.loc[start_mask]
        self.journeys = self.journeys.loc[end_mask]
        logging.info(
            f"Number of journeys between {start} and {end}: {len(self.journeys)}"
        )

    def enrich_journeys(self):
        # Add next stop
        # ------------------------------------
        trip_id = self.journeys["trip_id"]
        self.journeys["next_stop_name"] = (
            self.journeys["stop_name"].shift(-1).where(trip_id.eq(trip_id.shift(-1)))
        )
        move = self.journeys.pop("next_stop_name")
        self.journeys.insert(10, "next_stop_name", move)
        # ------------------------------------

        # Add journey times
        # ------------------------------------
        self.journeys["arrival_time"] = pd.to_datetime(
            self.journeys["arrival_time"], format="%H:%M:%S", errors="coerce"
        )
        self.journeys["departure_time"] = pd.to_datetime(
            self.journeys["departure_time"], format="%H:%M:%S", errors="coerce"
        )
        arr_dt = self.journeys["arrival_time"]
        dep_dt = self.journeys["departure_time"]
        trip_id = self.journeys["trip_id"]
        self.journeys["journey_time"] = arr_dt - dep_dt.shift().where(
            trip_id.eq(trip_id.shift())
        )
        self.journeys["arrival_time"] = self.journeys["arrival_time"].dt.time
        self.journeys["departure_time"] = self.journeys["departure_time"].dt.time
        self.journeys["journey_time"] = self.journeys["journey_time"].dt.seconds
        move = self.journeys.pop("journey_time")
        self.journeys.insert(4, "journey_time", move)
        # ------------------------------------

        # Add text
        # ------------------------------------
        self.journeys["text"] = self.journeys.apply(MTAReference.row2text, axis=1)
        # ------------------------------------

    def get_trip_summary(self, trip: str, from_stop: Optional[str] = None) -> dict:
        """Returns a text summary of a trip, including a list of stops and scheduled arrival times,
        optionally filtered from a given stop."""

        df = self.journeys[
            [
                "trip_id",
                "trip_headsign",
                "route_id",
                "direction_id",
                "stop_id",
                "stop_name",
                "arrival_time",
            ]
        ]
        df = df[df["trip_id"] == trip]
        if len(df) == 0:
            return {}
        stops_at = []
        for _, row in df.iterrows():
            if from_stop:
                if from_stop in (row["stop_id"], row["stop_name"]):
                    stops_at.clear()
            stops_at.append(
                '"{}" ({})'.format(
                    row["stop_name"],
                    row["arrival_time"].strftime("%H:%M:%S"),
                )
            )
        res = {"Route": df["route_id"].iloc[0]}
        res["text"] = (
            'Route {} travelling {} with the headsign "{}" stops at, {}'.format(
                df["route_id"].iloc[0],
                str(df["direction_id"].iloc[0]).lower(),
                df["trip_headsign"].iloc[0],
                ", ".join(stops_at),
            )
        )
        return res

    def get_route_name(self, route_id: str) -> str:
        """Returns route name for route id"""
        name: str = None
        try:
            name = self.routes.loc[route_id].iloc[0]
        except KeyError:
            logging.debug(f"Unknown route: {route_id}")
        return name

    def get_stop_name(self, stop_id: str) -> str:
        """Returns stop name for stop id"""
        name: str = None
        try:
            name = self.stops.loc[stop_id].iloc[0]
        except KeyError:
            logging.debug(f"Unknown stop: {stop_id}")
        return name

    def get_direction(self, stop_id: str) -> str:
        """Returns direction of travel (North or South) for stop id."""
        direction: str = ""
        if stop_id[-1].upper() == "N":
            direction = "North"
        elif stop_id[-1].upper() == "S":
            direction = "South"
        return direction

    @staticmethod
    def row2text(row: Series) -> str:
        """Returns a text representation of a journeys row."""
        text = []
        text.append(
            'Route {} "{}" travelling {} will arrive at stop "{}" at {}.'.format(
                row.route_id,
                row.route_long_name,
                str(row.direction_id).lower(),
                row.stop_name,
                row.arrival_time,
            )
        )
        if not pd.isnull(row.next_stop_name):
            text.append(f" The next stop is {row.next_stop_name}.")
        return "".join(text)

    @staticmethod
    def load_ref(file: str, cols: list[str] = ()) -> DataFrame:
        df = pd.read_csv(file)
        if len(cols) > 0:
            df = df[cols]
        return df

    @staticmethod
    def load_routes() -> DataFrame:
        routes_cols: list[str] = ["route_id", "route_long_name", "route_desc"]
        routes_df: DataFrame = MTAReference.load_ref(
            os.path.join(MTAReference.base_dir, "routes.txt"), routes_cols
        )
        routes_df.set_index("route_id", inplace=True)
        logging.info(f"Routes: {len(routes_df)}")
        return routes_df

    @staticmethod
    def load_trips() -> DataFrame:
        trips_cols: list[str] = [
            "route_id",
            "trip_id",
            "trip_headsign",
            "service_id",
            "direction_id",
        ]
        trips_df: DataFrame = MTAReference.load_ref(
            os.path.join(MTAReference.base_dir, "trips.txt"), trips_cols
        )
        trips_df["direction_id"] = trips_df.apply(
            lambda row: "South" if row.direction_id == 1 else "North", axis=1
        )
        logging.info(f"Trips: {len(trips_df)}")
        return trips_df

    @staticmethod
    def load_stops() -> DataFrame:
        stops_cols: list[str] = ["stop_id", "stop_name"]
        stops_df: DataFrame = MTAReference.load_ref(
            os.path.join(MTAReference.base_dir, "stops.txt"), stops_cols
        )
        stops_df.set_index("stop_id", inplace=True)
        logging.info(f"Stops: {len(stops_df)}")
        return stops_df

    @staticmethod
    def load_stop_times() -> DataFrame:
        stop_times_df: DataFrame = MTAReference.load_ref(
            os.path.join(MTAReference.base_dir, "stop_times.txt")
        )
        logging.info(f"Stop times: {len(stop_times_df)}")
        return stop_times_df
