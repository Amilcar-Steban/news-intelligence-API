from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests, os
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="ingestor")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
NEWSAPI_URL = "https://newsapi.org/v2/top-headlines?q=technology&language=en"

DB_DSN = os.getenv("POSTGRES_DSN", "postgresql://user:pass@postgres:5432/newsdb")
EMBEDDER_URL = os.getenv("EMBEDDER_URL", "http://embedder:8000/embed")
VECTORSTORE_URL = os.getenv("VECTORSTORE_URL", "http://vectorstore:8000/upsert")

class ArticleIn(BaseModel):
    title: str
    body: str
    url: str

def get_db():
    return psycopg2.connect(DB_DSN, cursor_factory=RealDictCursor)

@app.post("/fetch_remote")
def fetch_remote():
    headers = {"Authorization": NEWSAPI_KEY} if NEWSAPI_KEY else {}
    resp = requests.get(NEWSAPI_URL, headers=headers)
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail="failed to fetch")
    items = resp.json().get("articles", [])
    inserted = 0
    conn = get_db()
    cur = conn.cursor()
    for it in items:
        title = it.get("title")
        body = it.get("content") or it.get("description") or ""
        url = it.get("url")
        # insert minimal
        cur.execute("INSERT INTO articles (title, body, url) VALUES (%s,%s,%s) RETURNING id", (title, body, url))
        art_id = cur.fetchone()[0]
        conn.commit()
        # call embedder
        emb = requests.post(EMBEDDER_URL, json={"id": str(art_id), "text": f"{title}\n\n{body}"})
        if emb.status_code == 200:
            vec = emb.json()["vector"]
            # upsert vector
            requests.post(VECTORSTORE_URL, json=[{"id": str(art_id), "vector": vec, "payload": {"title": title, "url": url}}])
        inserted += 1
    cur.close()
    conn.close()
    return {"inserted": inserted}
