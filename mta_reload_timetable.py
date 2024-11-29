import os
import logging
import cohere
from mta_reference import MTAReference
from pandas import DataFrame
from datetime import date, datetime
from dotenv import load_dotenv
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from qdrant_client import QdrantClient
from qdrant_client.models import Batch, Distance, VectorParams

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s]  %(message)s",
    handlers=[logging.StreamHandler()],
)

load_dotenv()
qdrant_url = os.getenv("QDRANT_URL")
qdrant_collection = os.getenv("QDRANT_COLLECTION")
cohere_key = os.getenv("COHERE_API_KEY")


def recreate_collection(journeys: DataFrame):
    co = cohere.Client(cohere_key)
    client = QdrantClient(url=qdrant_url)

    buf_max: int = 96
    id_buf: list[int] = []
    text_buf: list[str] = []
    meta_buf: list[dict] = []

    try:
        client.delete_collection(qdrant_collection)
        client.create_collection(
            collection_name=qdrant_collection,
            vectors_config=VectorParams(size=1024, distance=Distance.COSINE),
        )

        with logging_redirect_tqdm():
            for id, row in tqdm(
                journeys.iterrows(), total=journeys.shape[0], colour="#F9944F"
            ):
                id_buf.append(int(id))
                text_buf.append(row.text)
                arrival_time: datetime
                try:
                    arrival_time = datetime.combine(date.min, row.arrival_time)
                except:
                    continue
                meta_buf.append(
                    {
                        "trip_id": row.trip_id,
                        "stop_id": row.stop_id,
                        "arrival_time": arrival_time,
                        "text": row.text,
                    }
                )
                # Cohere embed max number of text per api call is 96
                if len(text_buf) == buf_max:
                    # Generating the embeddings
                    embeddings = co.embed(
                        model="embed-english-v3.0",  # 1024
                        input_type="search_document",
                        texts=text_buf,
                    ).embeddings
                    # Insert into vector store
                    client.upsert(
                        collection_name=qdrant_collection,
                        points=Batch(ids=id_buf, vectors=embeddings, payloads=meta_buf),
                    )
                    id_buf.clear()
                    text_buf.clear()
                    meta_buf.clear()
    finally:
        client.close()


if __name__ == "__main__":
    ref = MTAReference()
    user_input = input("Do you want to continue? (yes/no): ")
    if user_input.lower() in ["yes", "y"]:
        recreate_collection(ref.get_journeys())
