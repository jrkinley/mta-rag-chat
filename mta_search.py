import os
import sys
import logging
import cohere
from mta_reference import MTAReference
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from cohere import ChatDocument
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, DatetimeRange

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s]  %(message)s",
    handlers=[logging.StreamHandler()],
)

load_dotenv()
qdrant_url = os.getenv("QDRANT_URL")
qdrant_collection = os.getenv("QDRANT_COLLECTION")
cohere_key = os.getenv("COHERE_API_KEY")
ref = MTAReference()


def retrieve(query: str) -> list[ChatDocument]:
    """Retrieves similar texts from the QDrant collection. Applies a time range
    filter to narrow the results down for the next 15 minutes.
    """
    co = cohere.Client(cohere_key)
    client = QdrantClient(url=qdrant_url)

    time_now = datetime.combine(date.min, datetime.now().time())
    time_plus = time_now + timedelta(minutes=15)
    filter = Filter(
        must=[
            FieldCondition(
                key="arrival_time",
                range=DatetimeRange(
                    gte=time_now.isoformat(),
                    lte=time_plus.isoformat(),
                ),
            )
        ]
    )
    try:
        results = client.search(
            collection_name=qdrant_collection,
            query_vector=co.embed(
                model="embed-english-v3.0",
                input_type="search_query",
                texts=[query],
            ).embeddings[0],
            query_filter=filter,
            limit=5,
        )
    finally:
        client.close()
    results = sorted(results, key=lambda x: x.payload.get("arrival_time"))
    docs = []
    for result in results:
        # Strip date from datetime
        ts = result.payload.get("arrival_time")
        if ts:
            ts = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
            ts = ts.strftime("%H:%M:%S")
        docs.append({"arrival_time": ts, "text": result.payload.get("text")})

        # # Fetch future stops for trip
        trip = result.payload.get("trip_id")
        stop = result.payload.get("stop_id")
        trip_stops = ref.get_trip_summary(trip, stop)
        docs.append(trip_stops)
    return docs


if __name__ == "__main__":
    if len(sys.argv) == 0:
        logging.error("Please ask a question!")
        sys.exit(1)
    query = sys.argv[1]
    logging.info(f"Question: {query}")
    results = retrieve(query)
    for r in results:
        logging.info(f"{r}")
