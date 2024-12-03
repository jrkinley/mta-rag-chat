import os
import sys
import cohere
from mta_reference import MTAReference
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from cohere import Client, ChatDocument
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, DatetimeRange

load_dotenv()
qdrant_url = os.getenv("QDRANT_URL")
qdrant_collection = os.getenv("QDRANT_COLLECTION")
cohere_key = os.getenv("COHERE_API_KEY")
ref = MTAReference()


def retrieve(query: str, co: Client, qd: QdrantClient) -> list[ChatDocument]:
    """Retrieves similar texts from the QDrant collection. Applies a time range
    filter to narrow the results down for the next 15 minutes.
    """

    # Timetable time filter (no date)
    dt = datetime.now()
    tt_min = datetime.combine(date.min, dt.time())
    tt_max = tt_min + timedelta(minutes=15)

    # Realtime time filter
    rt_min = dt
    rt_max = dt + timedelta(minutes=15)

    filter = Filter(
        should=[
            FieldCondition(
                key="arrival_time",
                range=DatetimeRange(
                    gte=tt_min,
                    lte=tt_max,
                ),
            ),
            FieldCondition(
                key="arrival_time",
                range=DatetimeRange(
                    gte=rt_min,
                    lte=rt_max,
                ),
            ),
        ]
    )
    results = qd.search(
        collection_name=qdrant_collection,
        query_vector=co.embed(
            model="embed-english-v3.0",
            input_type="search_query",
            texts=[query],
        ).embeddings[0],
        query_filter=filter,
    )
    results = sorted(results, key=lambda x: x.payload.get("arrival_time"))
    docs = []
    for result in results:
        # Strip date from datetime
        ts = result.payload.get("arrival_time")
        if ts:
            ts = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
            ts = ts.strftime("%H:%M:%S")
        docs.append({"arrival_time": ts, "text": result.payload.get("text")})

        # Fetch future stops for trip
        trip = result.payload.get("trip_id")
        stop = result.payload.get("stop_id")
        trip_stops = ref.get_trip_summary(trip, stop)
        if trip_stops:
            docs.append(trip_stops)
    return docs


if __name__ == "__main__":
    if len(sys.argv) == 0:
        print("Please ask a question!")
        sys.exit(1)

    cohere_client = cohere.Client(cohere_key)
    qdrant_client = QdrantClient(url=qdrant_url)

    query = sys.argv[1]
    print(f"Question: {query}")
    try:
        results = retrieve(query, cohere_client, qdrant_client)
    finally:
        qdrant_client.close()
    for r in results:
        print(f"{r}")
