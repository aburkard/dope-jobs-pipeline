"""Compute Meili document vectors in the pipeline instead of during Meili indexing.

The live `jobs` index has a composite `default` embedder: Meili embeds search
queries itself (local TEI), but every indexed document used to be embedded by
Meili through a proxy to OpenRouter, inside the indexing task. That made loads
slow and fragile. Instead, the pipeline now renders the same document text,
embeds it with the same model/provider/dimensions, and sends
`_vectors: {"default": {"embeddings": ..., "regenerate": false}}`. Meili then
stores the vector without calling any embedder, and never regenerates it.

Keep MODEL/DIMENSIONS/normalization identical to the Meili search embedder
(`perplexity/pplx-embed-v1-0.6b`, 512 dims, L2-normalized); Meili cannot detect
a mismatch between document and query vectors.
"""
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor

import requests

EMBEDDER_NAME = "default"
OPENROUTER_URL = os.environ.get("OPENROUTER_EMBEDDINGS_URL", "https://openrouter.ai/api/v1/embeddings")
MODEL = os.environ.get("PIPELINE_EMBED_MODEL", "perplexity/pplx-embed-v1-0.6b")
DIMENSIONS = int(os.environ.get("PIPELINE_EMBED_DIMENSIONS", "512"))
BATCH_SIZE = 64
CONCURRENCY = 4
RETRIES = 4
TIMEOUT_SECONDS = 60
DOCUMENT_TEMPLATE_MAX_BYTES = 8000


def embeddings_enabled() -> bool:
    return bool(os.environ.get("OPENROUTER_API_KEY"))


def _liquid_truthy(value) -> bool:
    # Liquid treats only nil and false as falsy; "" and [] are truthy.
    return value is not None and value is not False


def _truncatewords(value, count: int) -> str:
    # liquid-rust splits on a single " " (not any whitespace), so newlines stay
    # inside words and consecutive spaces count as empty words.
    text = str(value)
    words = text.split(" ")
    if len(words) <= count:
        return text
    return " ".join(words[:count]) + "..."


def render_document_text(doc: dict) -> str:
    """Python rendering of the Meili indexing documentTemplate.

    Mirrors ops/apply_perplexity_meili_embedder.py in the dopejobs repo:
        {% if doc.title %}Job title: {{ doc.title }}.{% endif %}
        {% if doc.company %} Company: {{ doc.company }}.{% endif %}
        {% if doc.office_type %} Work setup: {{ doc.office_type }}.{% endif %}
        {% if doc.tagline %} Summary: {{ doc.tagline | truncatewords: 16 }}.{% endif %}
        {% if doc.description %} Description: {{ doc.description | truncatewords: 90 }}{% endif %}
    """
    lines = [
        f"Job title: {doc['title']}." if _liquid_truthy(doc.get("title")) else "",
        f" Company: {doc['company']}." if _liquid_truthy(doc.get("company")) else "",
        f" Work setup: {doc['office_type']}." if _liquid_truthy(doc.get("office_type")) else "",
        f" Summary: {_truncatewords(doc['tagline'], 16)}." if _liquid_truthy(doc.get("tagline")) else "",
        f" Description: {_truncatewords(doc['description'], 90)}" if _liquid_truthy(doc.get("description")) else "",
    ]
    text = "\n".join(lines)
    encoded = text.encode("utf-8")
    if len(encoded) > DOCUMENT_TEMPLATE_MAX_BYTES:
        text = encoded[:DOCUMENT_TEMPLATE_MAX_BYTES].decode("utf-8", errors="ignore")
    return text


def _normalize(vector: list[float]) -> list[float]:
    vector = vector[:DIMENSIONS]
    norm = math.sqrt(sum(x * x for x in vector)) or 1e-12
    return [x / norm for x in vector]


def _embed_batch(texts: list[str], session: requests.Session) -> list[list[float]]:
    last_error: Exception | None = None
    for attempt in range(RETRIES):
        try:
            response = session.post(
                OPENROUTER_URL,
                headers={"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}"},
                json={"model": MODEL, "input": texts, "dimensions": DIMENSIONS, "encoding_format": "float"},
                timeout=TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            data = sorted(response.json()["data"], key=lambda item: item["index"])
            if len(data) != len(texts):
                raise ValueError(f"expected {len(texts)} embeddings, got {len(data)}")
            vectors = [_normalize(item["embedding"]) for item in data]
            if any(len(vector) != DIMENSIONS for vector in vectors):
                raise ValueError(f"embedding dimensions differ from {DIMENSIONS}")
            return vectors
        except (requests.RequestException, ValueError, KeyError) as exc:
            last_error = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"embedding batch of {len(texts)} failed") from last_error


def embed_texts(texts: list[str]) -> list[list[float]]:
    batches = [texts[i:i + BATCH_SIZE] for i in range(0, len(texts), BATCH_SIZE)]
    with requests.Session() as session, ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        results = list(pool.map(lambda batch: _embed_batch(batch, session), batches))
    return [vector for batch in results for vector in batch]


def user_provided_vectors(embedding: list[float]) -> dict:
    return {EMBEDDER_NAME: {"embeddings": embedding, "regenerate": False}}


def attach_vectors(docs: list[dict]) -> bool:
    """Embed docs in place. Returns False (docs untouched) if embedding fails,
    so the load can fall back to Meili embedding them as before."""
    if not docs or not embeddings_enabled():
        return False
    try:
        vectors = embed_texts([render_document_text(doc) for doc in docs])
    except RuntimeError as exc:
        print(f"  Warning: pipeline embedding failed ({exc.__cause__!r}); Meili will embed this batch")
        return False
    for doc, vector in zip(docs, vectors):
        doc["_vectors"] = user_provided_vectors(vector)
    return True
