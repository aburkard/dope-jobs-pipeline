"""Benchmark a running TEI server: single-query latency and (optionally) document throughput.

Throwaway embedding benchmark (no secrets, no production data). Documents are synthetic text with the
word counts of real templated job documents (bench/doc_word_counts.json), since cost depends on length.

usage: bench_tei.py NAME URL [--prefix P] [--docs] [--reference-model REPO]
"""
import argparse, json, os, random, statistics, time
from concurrent.futures import ThreadPoolExecutor
import requests

p = argparse.ArgumentParser()
p.add_argument("name"); p.add_argument("url")
p.add_argument("--prefix", default="")
p.add_argument("--docs", action="store_true")
p.add_argument("--reference-model", default="")
a = p.parse_args()
HERE = os.path.dirname(os.path.abspath(__file__))
queries = [q.strip() for q in open(f"{HERE}/queries.txt") if q.strip()]
s = requests.Session()


def embed(inputs):
    r = s.post(f"{a.url}/embed", json={"inputs": inputs, "truncate": True}, timeout=300)
    r.raise_for_status()
    return r.json()


res = {"name": a.name}
v = embed(a.prefix + queries[0])
res["dims"] = len(v[0])
for q in queries[:10]:
    embed(a.prefix + q)  # warm-up
lat = []
for _ in range(3):
    for q in queries:
        t = time.perf_counter(); embed(a.prefix + q); lat.append((time.perf_counter() - t) * 1000)
lat.sort()
res.update(query_p50_ms=round(statistics.median(lat), 1), query_p95_ms=round(lat[int(0.95 * len(lat))], 1),
           query_max_ms=round(lat[-1], 1), n=len(lat))

if a.reference_model:  # check TEI output matches sentence-transformers (catches missing Dense/normalize)
    import numpy as np
    from sentence_transformers import SentenceTransformer
    m = SentenceTransformer(a.reference_model, device="cpu")
    ref = m.encode([a.prefix + q for q in queries[:20]], normalize_embeddings=True)
    tei = np.array([embed(a.prefix + q)[0] for q in queries[:20]], dtype=np.float32)
    tei /= np.linalg.norm(tei, axis=1, keepdims=True)
    res["cos_vs_sentence_transformers"] = round(float(np.mean(np.sum(ref * tei, axis=1))), 5) if ref.shape == tei.shape else f"shape {tei.shape} vs {ref.shape}"

if a.docs:
    words = "engineer team product customer data build manage support develop design senior remote experience " \
            "systems sales operations growth platform software services clients projects quality".split()
    random.seed(0)
    lengths = json.load(open(f"{HERE}/doc_word_counts.json"))[:2048]
    docs = [" ".join(random.choice(words) for _ in range(n)) for n in lengths]
    batches = [docs[i:i + 32] for i in range(0, len(docs), 32)]
    for workers in (1, 4):
        t = time.perf_counter()
        with ThreadPoolExecutor(workers) as pool:
            list(pool.map(embed, batches))
        res[f"docs_per_s_{workers}_clients"] = round(len(docs) / (time.perf_counter() - t), 1)

print(json.dumps(res))
with open(os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null"), "a") as f:
    f.write(f"### {a.name}\n```\n{json.dumps(res, indent=1)}\n```\n")
