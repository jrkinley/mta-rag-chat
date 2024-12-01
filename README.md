# cohere-mta-chat

```
docker compose up -d
[+] Running 5/5
 ✔ Network redpanda_network    Created
 ✔ Volume "redpanda-0"         Created
 ✔ Container redpanda-0        Started
 ✔ Container qdrant            Started
 ✔ Container redpanda-console  Started
```

```
python mta_reload_timetable.py
```

http://localhost:6333/dashboard#/collections

```
python mta_realtime.py
rpk topic consume mta-gtfs-realtime -X brokers=localhost:19092 | jq
rpk connect run --log.level debug --env-file .env mta_embeddings.yaml
```

http://localhost:8080/topics

```
python mta_search.py "What is the next train to arrive at 50 St?"
```


# Questions:
#   What is the next train to arrive at 50 St?
#   Does the train travelling south stop at Franklin St?
#   What is the headsign on this train?
