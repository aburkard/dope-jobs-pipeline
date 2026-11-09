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

import index_profiles

EMBEDDER_NAME = "default"
OPENROUTER_URL = os.environ.get("OPENROUTER_EMBEDDINGS_URL", "https://openrouter.ai/api/v1/embeddings")
MODEL = os.environ.get("PIPELINE_EMBED_MODEL", "perplexity/pplx-embed-v1-0.6b")
DIMENSIONS = int(os.environ.get("PIPELINE_EMBED_DIMENSIONS", "512"))
BATCH_SIZE = 64
CONCURRENCY = 4
RETRIES = 4
TIMEOUT_SECONDS = 60
DOCUMENT_TEMPLATE_MAX_BYTES = 8000
# OpenRouter answers 401 (bad key), 402 (credit/limit exhausted) or 403 (key disabled/forbidden).
# None of these heal on retry, and Meili's own indexing embedder uses the same key, so falling
# back to Meili embedding would only fail again, slowly.
AUTH_STATUS_CODES = (401, 402, 403)


# v2 profile ("voyage docs + student queries"): voyage-4-lite native 1024d document vectors,
# L2-normalized, projected with the pinned clean P (1024 -> 256), L2-normalized again.
V2_MODEL = "mongodb/voyage-4-lite"
V2_INPUT_TYPE = "document"
V2_PROJECTION_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models", "proj_clean_voy4_1024_to_256.npy")
V2_PROJECTION_SHA256 = "3788076b4bf3d0a9b323878d0fa528d48948a48227482cb13ee66d30b8b1fe5f"
_projection = None


def v2_projection():
    """The pinned clean projection P, float32 [1024, 256]. Refuses a file whose sha256 differs."""
    global _projection
    if _projection is None:
        import hashlib
        import numpy as np
        with open(V2_PROJECTION_PATH, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        if digest != V2_PROJECTION_SHA256:
            raise RuntimeError(f"v2 projection sha256 {digest} != pinned {V2_PROJECTION_SHA256}")
        projection = np.load(V2_PROJECTION_PATH).astype(np.float32)
        if projection.shape != (1024, index_profiles.V2_DIMENSIONS):
            raise RuntimeError(f"v2 projection shape {projection.shape} != (1024, {index_profiles.V2_DIMENSIONS})")
        _projection = projection
    return _projection


def project_v2(raw: list[list[float]]) -> list[list[float]]:
    """x / ||x||, then @ P, then / ||.||: exactly the recipe the full-scale gate was judged on."""
    import numpy as np
    x = np.asarray(raw, dtype=np.float32)
    x /= np.maximum(np.linalg.norm(x, axis=1, keepdims=True), 1e-12)
    y = x @ v2_projection()
    y /= np.maximum(np.linalg.norm(y, axis=1, keepdims=True), 1e-12)
    return y.tolist()


class EmbeddingRequiredError(RuntimeError):
    """v2 profile: document vectors could not be computed. Don't let Meili embed (its embedder is the
    query encoder); the load must stop and leave the jobs pending."""


class EmbeddingAuthError(RuntimeError):
    """OpenRouter rejected the key (auth or spending limit). Not retryable; the load must stop."""


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
            v2 = index_profiles.active_profile() == "v2"
            body = ({"model": V2_MODEL, "input": texts, "input_type": V2_INPUT_TYPE, "encoding_format": "float"}
                    if v2 else {"model": MODEL, "input": texts, "dimensions": DIMENSIONS, "encoding_format": "float"})
            response = session.post(
                OPENROUTER_URL,
                headers={"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}"},
                json=body,
                timeout=TIMEOUT_SECONDS,
            )
            if response.status_code in AUTH_STATUS_CODES:
                raise EmbeddingAuthError(
                    f"OpenRouter key limit/auth: HTTP {response.status_code} from embeddings endpoint")
            response.raise_for_status()
            data = sorted(response.json()["data"], key=lambda item: item["index"])
            if len(data) != len(texts):
                raise ValueError(f"expected {len(texts)} embeddings, got {len(data)}")
            if v2:
                raw = [item["embedding"] for item in data]
                if any(len(vector) != 1024 for vector in raw):
                    raise ValueError("voyage-4-lite returned non-1024d vectors")
                return project_v2(raw)
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
    so the load can fall back to Meili embedding them as before.

    Raises EmbeddingAuthError on OpenRouter 401/402/403: the fallback uses the same key."""
    v2 = index_profiles.active_profile() == "v2"
    if not docs:
        return False
    if not embeddings_enabled():
        if v2:
            raise EmbeddingRequiredError("v2 profile needs OPENROUTER_API_KEY to compute document vectors")
        return False
    try:
        vectors = embed_texts([render_document_text(doc) for doc in docs])
    except EmbeddingAuthError:
        raise
    except RuntimeError as exc:
        if v2:
            raise EmbeddingRequiredError(f"document embedding failed ({exc.__cause__!r}); not falling back to Meili") from exc
        print(f"  Warning: pipeline embedding failed ({exc.__cause__!r}); Meili will embed this batch")
        return False
    for doc, vector in zip(docs, vectors):
        doc["_vectors"] = user_provided_vectors(vector)
    return True
