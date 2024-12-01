import os
import sys
import logging
import cohere
import uuid
from mta_search import retrieve
from dotenv import load_dotenv
from cohere import Client, ChatDocument
from qdrant_client import QdrantClient

logger = logging.getLogger()
logger.propagate = False

load_dotenv()
qdrant_url = os.getenv("QDRANT_URL")
qdrant_collection = os.getenv("QDRANT_COLLECTION")
cohere_key = os.getenv("COHERE_API_KEY")

system_message = """
## Task and Context
You are a chatbot who helps passengers plan their journeys on the New York Subway.

## Style Guide
Always respond in a friendly but typical New Yorker accent.
"""


def chat(co: Client, qd: QdrantClient, conversation_id: str):
    while True:
        human_message = input("User: ")
        if human_message.lower() == "quit":
            print("Ending chat.")
            break
        ai_response = co.chat(
            message=human_message,
            model="command-r",
            search_queries_only=True,
        )
        if ai_response.search_queries:
            print("Retrieving information...", end="")
            # Retrieve similar documents from vector store
            documents = []
            for query in ai_response.search_queries:
                documents.extend(retrieve(query.text, co, qd))
            ai_response = co.chat_stream(
                message=human_message,
                preamble=system_message,
                model="command-r",
                documents=documents,
                conversation_id=conversation_id,
            )
        else:
            ai_response = co.chat_stream(
                message=human_message,
                model="command-r",
                conversation_id=conversation_id,
            )

        print("\nChatbot:")
        for event in ai_response:
            if event.event_type == "text-generation":
                print(event.text, end="")
        print(f"\n{'-'*75}\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        conversation_id = sys.argv[1]
    else:
        conversation_id = str(uuid.uuid4())
    cohere_client = cohere.Client(cohere_key)
    qdrant_client = QdrantClient(url=qdrant_url)
    try:
        chat(cohere_client, qdrant_client, conversation_id)
    finally:
        qdrant_client.close()
