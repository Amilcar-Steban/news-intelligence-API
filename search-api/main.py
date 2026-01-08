from fastapi import FastAPI
from pydantic import BaseModel
import requests
import os

app = FastAPI(title="search-api")
EMBEDDER_URL = os.getenv("EMBEDDER_URL", "http://embedder:8000/embed")
VECTORSTORE_SEARCH_URL = os.getenv("VECTORSTORE_SEARCH_URL", "http://vectorstore:8000/search")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://user:pass@postgres:5432/newsdb")
import psycopg2
from psycopg2.extras import RealDictCursor

class QueryIn(BaseModel):
    q: str
    top_k: int = 5

def get_db():
    return psycopg2.connect(POSTGRES_DSN, cursor_factory=RealDictCursor)

@app.post("/search")
def search(q: QueryIn):
    emb = requests.post(EMBEDDER_URL, json={"text": q.q})
    vector = emb.json()["vector"]
    vs = requests.post(VECTORSTORE_SEARCH_URL, json={"vector": vector, "top_k": q.top_k})
    ids = [r["id"] for r in vs.json()["results"]]
    # fetch from postgres
    conn = get_db()
    cur = conn.cursor()
    if not ids:
        return {"results": []}
    cur.execute("SELECT id, title, url, body FROM articles WHERE id = ANY(%s)", (ids,))
    rows = cur.fetchall()
    # preserve order by ids
    id_map = {str(r["id"]): r for r in rows}
    results = [id_map[i] for i in ids if i in id_map]
    return {"results": results}
