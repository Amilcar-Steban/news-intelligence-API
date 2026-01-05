from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, Filter
import os

app = FastAPI(title="vectorstore")
QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", 6333))
COLLECTION = os.getenv("QDRANT_COLLECTION", "news")

client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

class UpsertItem(BaseModel):
    id: str
    vector: list[float]
    payload: dict | None = None

class SearchPayload(BaseModel):
    vector: list[float]
    top_k: int = 5
    filter: dict | None = None

@app.post("/upsert")
def upsert(items: list[UpsertItem]):
    points = [PointStruct(id=item.id, vector=item.vector, payload=item.payload or {}) for item in items]
    client.recreate_collection(collection_name=COLLECTION, vectors_config={"size": len(points[0].vector), "distance": "Cosine"}) if not client.get_collection(COLLECTION) else None
    client.upload_collection(collection_name=COLLECTION, points=points, parallel=4)
    return {"status": "ok", "inserted": len(points)}

@app.post("/search")
def search(p: SearchPayload):
    hits = client.search(collection_name=COLLECTION, query_vector=p.vector, limit=p.top_k)
    results = [{"id": h.id, "score": h.score, "payload": h.payload} for h in hits]
    return {"results": results}
