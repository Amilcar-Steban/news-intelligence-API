from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import os

app = FastAPI(title="embedder")
MODEL_NAME = os.getenv("EMBED_MODEL", "all-MiniLM-L6-v2")

class TextPayload(BaseModel):
    id: str | None = None
    text: str

model = SentenceTransformer(MODEL_NAME)

@app.post("/embed")
def embed(payload: TextPayload):
    if not payload.text:
        raise HTTPException(status_code=400, detail="text required")
    vec = model.encode(payload.text, show_progress_bar=False).tolist()
    return {"id": payload.id, "vector": vec}
