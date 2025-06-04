"""
GTFS Data Access Module
"""

import os
import pandas as pd
from typing import Optional, Dict, List, Generator
from pandas import DataFrame, Series
from datetime import datetime, timedelta
from functools import lru_cache


# Configuration
DEFAULT_GTFS_DIR = "./gtfs-reference"


# =============================================================================
# Data Loading and Caching
# =============================================================================


@lru_cache(maxsize=1)
def load_routes(gtfs_dir: str = DEFAULT_GTFS_DIR) -> DataFrame:
    """Load and cache routes data."""
    cols = ["route_id", "route_long_name", "route_desc"]
    df = pd.read_csv(os.path.join(gtfs_dir, "routes.txt"), usecols=cols)
    df.set_index("route_id", inplace=True)
    return df


@lru_cache(maxsize=1)
def load_trips(gtfs_dir: str = DEFAULT_GTFS_DIR) -> DataFrame:
    """Load and cache trips data."""
    cols = ["route_id", "trip_id", "trip_headsign", "service_id", "direction_id"]
    df = pd.read_csv(os.path.join(gtfs_dir, "trips.txt"), usecols=cols)
    df["direction_id"] = df["direction_id"].map({0: "North", 1: "South"})
    return df


@lru_cache(maxsize=1)
def load_stops(gtfs_dir: str = DEFAULT_GTFS_DIR) -> DataFrame:
    """Load and cache stops data."""
    cols = ["stop_id", "stop_name"]
    df = pd.read_csv(os.path.join(gtfs_dir, "stops.txt"), usecols=cols)
    df.set_index("stop_id", inplace=True)
    return df


@lru_cache(maxsize=1)
def load_stop_times(gtfs_dir: str = DEFAULT_GTFS_DIR) -> DataFrame:
    """Load and cache stop times data."""
    return pd.read_csv(os.path.join(gtfs_dir, "stop_times.txt"))


# =============================================================================
# Data Processing
# =============================================================================


def create_journeys_base(gtfs_dir: str = DEFAULT_GTFS_DIR) -> DataFrame:
    """Create base journeys dataframe by joining all GTFS tables."""
    stop_times = load_stop_times(gtfs_dir)
    trips = load_trips(gtfs_dir)
    stops = load_stops(gtfs_dir)
    routes = load_routes(gtfs_dir)

    journeys = stop_times.merge(trips, on="trip_id", how="left")
    journeys = journeys.merge(stops, on="stop_id", how="left")
    journeys = journeys.merge(routes, on="route_id", how="left")
    journeys = journeys.sort_values(by=["trip_id", "stop_sequence"])

    return journeys


def filter_journeys(
    journeys: DataFrame,
    route_ids: Optional[List[str]] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    direction: Optional[str] = None,
    service_id: Optional[str] = None,
) -> DataFrame:
    """
    Filter journeys based on flexible criteria.

    Args:
        journeys: Base journeys dataframe
        route_ids: List of route IDs to include (e.g., ["1", "2", "3"])
        start_time: Start time in HH:MM:SS format
        end_time: End time in HH:MM:SS format
        direction: "North" or "South"
        service_id: Service type ("Sunday", "Saturday", or "Weekday")
    """
    df = journeys.copy()

    # Route filtering
    if route_ids:
        df = df[df["route_id"].isin(route_ids)]

    # Direction filtering
    if direction:
        df = df[df["direction_id"] == direction]

    # Service type filtering
    if service_id:
        df = df[df["service_id"] == service_id]

    # Time filtering
    if start_time or end_time:
        df["arrival_time_parsed"] = pd.to_datetime(
            df["arrival_time"], format="%H:%M:%S", errors="coerce"
        ).dt.time

        if start_time:
            start_mask = df["arrival_time_parsed"] >= pd.Timestamp(start_time).time()
            df = df[start_mask]

        if end_time:
            end_mask = df["arrival_time_parsed"] <= pd.Timestamp(end_time).time()
            df = df[end_mask]

        df = df.drop("arrival_time_parsed", axis=1)

    return df


def enrich_journeys(journeys: DataFrame) -> DataFrame:
    """Add computed columns to journeys dataframe."""
    df = journeys.copy()

    # Add next stop
    trip_id = df["trip_id"]
    df["next_stop_name"] = (
        df["stop_name"].shift(-1).where(trip_id.eq(trip_id.shift(-1)))
    )

    df["arrival_time_epoch"] = pd.to_datetime(
        df["arrival_time"], format="%H:%M:%S", errors="coerce"
    ).astype("int64")

    df["departure_time_epoch"] = pd.to_datetime(
        df["departure_time"], format="%H:%M:%S", errors="coerce"
    ).astype("int64")

    return df


# =============================================================================
# High-level Query Functions
# =============================================================================


def get_journey_by_id(trip_id: str, gtfs_dir: str = DEFAULT_GTFS_DIR) -> DataFrame:
    """Get all journey records for a specific trip ID."""
    journeys = create_journeys_base(gtfs_dir)
    filtered = journeys[journeys["trip_id"] == trip_id].copy()
    enriched = enrich_journeys(filtered).sort_values("stop_sequence")
    return enriched


def get_current_journeys(
    route_ids: List[str] = ["1"], hours_ahead: int = 3, gtfs_dir: str = DEFAULT_GTFS_DIR
) -> DataFrame:
    """Get journeys for specified routes in the next N hours."""
    now = datetime.now()
    start_time = now.strftime("%H:%M:%S")
    end_time = (now + timedelta(hours=hours_ahead)).strftime("%H:%M:%S")

    # Determine current service type based on day of week
    weekday = now.strftime("%A")
    if weekday == "Sunday":
        service_id = "Sunday"
    elif weekday == "Saturday":
        service_id = "Saturday"
    else:
        service_id = "Weekday"

    journeys = create_journeys_base(gtfs_dir)
    filtered = filter_journeys(
        journeys,
        route_ids=route_ids,
        start_time=start_time,
        end_time=end_time,
        service_id=service_id,
    )
    enriched = enrich_journeys(filtered)

    return enriched


def get_journeys_json(
    route_ids: Optional[List[str]] = None,
    batch_size: int = 1000,
    gtfs_dir: str = DEFAULT_GTFS_DIR,
) -> Generator[List[Dict], None, None]:
    """
    Generator function to yield batches of JSON-formatted journey documents.
    """
    journeys = create_journeys_base(gtfs_dir)

    if route_ids:
        journeys = filter_journeys(journeys, route_ids=route_ids)

    # Process in batches
    for i in range(0, len(journeys), batch_size):
        batch = journeys.iloc[i : i + batch_size].copy()
        enriched_batch = enrich_journeys(batch)

        # Convert batch to list of JSON documents
        docs = []
        for _, row in enriched_batch.iterrows():
            doc = {
                "id": f"{row['trip_id']}_{row['stop_sequence']}",
                "trip_id": row["trip_id"],
                "service_id": row["service_id"],
                "route_id": row["route_id"],
                "route_name": row["route_long_name"],
                "direction": row["direction_id"],
                "trip_headsign": row["trip_headsign"],
                "stop_id": row["stop_id"],
                "stop_sequence": int(row["stop_sequence"]),
                "stop_name": row["stop_name"],
                "next_stop_name": row["next_stop_name"],
                "arrival_time": row["arrival_time"],
                "arrival_time_epoch": row["arrival_time_epoch"],
            }
            docs.append(doc)
        yield docs


def get_trips_json(
    route_ids: Optional[List[str]] = None,
    batch_size: int = 1000,
    gtfs_dir: str = DEFAULT_GTFS_DIR,
) -> Generator[List[Dict], None, None]:
    """
    Generator function to yield batches of JSON-formatted trip summary documents.
    """
    journeys = create_journeys_base(gtfs_dir)
    journeys = enrich_journeys(journeys)

    if route_ids:
        journeys = filter_journeys(journeys, route_ids=route_ids)

    trip_ids = journeys["trip_id"].unique()

    # Process in batches
    for i in range(0, len(trip_ids), batch_size):
        batch_trip_ids = trip_ids[i : i + batch_size]
        docs = []

        for trip_id in batch_trip_ids:
            trip_summary = get_trip_summary(trip_id, journeys=journeys)
            if trip_summary:
                docs.append(trip_summary)

        yield docs


def get_trip_summary(
    trip_id: str,
    journeys: Optional[DataFrame] = None,
    from_stop: Optional[str] = None,
    gtfs_dir: str = DEFAULT_GTFS_DIR,
) -> Dict:
    """Get a summary of a specific trip."""

    if journeys is None:
        journeys = create_journeys_base(gtfs_dir)
        journeys = enrich_journeys(journeys)

    trip_data = journeys[journeys["trip_id"] == trip_id].sort_values("stop_sequence")
    if trip_data.empty:
        return {}

    # Build stops list
    stops_at = []
    include_stop = from_stop is None

    for _, row in trip_data.iterrows():
        if from_stop and (from_stop in (row["stop_id"], row["stop_name"])):
            include_stop = True
            stops_at.clear()

        if include_stop:
            stops_at.append(
                {
                    "stop_sequence": row["stop_sequence"],
                    "stop_name": row["stop_name"],
                    "arrival_time": row["arrival_time"],
                }
            )

    first_row = trip_data.iloc[0]
    return {
        "id": first_row["trip_id"],
        "route": first_row["route_id"],
        "route_name": first_row["route_long_name"],
        "direction": first_row["direction_id"],
        "headsign": first_row["trip_headsign"],
        "stops": stops_at,
    }


def get_route_name(route_id: str, gtfs_dir: str = DEFAULT_GTFS_DIR) -> Optional[str]:
    """Get route name for a route ID."""
    try:
        routes = load_routes(gtfs_dir)
        return routes.loc[route_id, "route_long_name"]
    except KeyError:
        return None


def get_stop_name(stop_id: str, gtfs_dir: str = DEFAULT_GTFS_DIR) -> Optional[str]:
    """Get stop name for a stop ID."""
    try:
        stops = load_stops(gtfs_dir)
        return stops.loc[stop_id, "stop_name"]
    except KeyError:
        return None


def get_direction_from_stop_id(stop_id: str) -> str:
    """Get direction from stop ID suffix."""
    suffix = stop_id[-1].upper()
    return "North" if suffix == "N" else "South" if suffix == "S" else ""


# =============================================================================
# Cache Management
# =============================================================================


def clear_cache():
    """Clear all cached data. Useful when GTFS data is updated."""
    load_routes.cache_clear()
    load_trips.cache_clear()
    load_stops.cache_clear()
    load_stop_times.cache_clear()
