"""Latency / RSS / parity / doc-throughput benchmark for TEI-style /embed servers and in-process ONNX Runtime.

usage: bench.py http NAME URL [--container C] [--ref ST_DIR] [--docs]
       bench.py ort  NAME MODEL_DIR ONNX_FILE [--threads N]
Appends a JSON line to results.jsonl and a row to $GITHUB_STEP_SUMMARY.
"""
import argparse, json, os, random, statistics, subprocess, time
from concurrent.futures import ThreadPoolExecutor
import numpy as np, requests

p = argparse.ArgumentParser()
p.add_argument("mode"); p.add_argument("name"); p.add_argument("a"); p.add_argument("b", nargs="?")
p.add_argument("--container"); p.add_argument("--ref"); p.add_argument("--docs", action="store_true")
p.add_argument("--threads", type=int, default=4)
a = p.parse_args()
HERE = os.path.dirname(os.path.abspath(__file__))
queries = [q.strip() for q in open(f"{HERE}/queries.txt") if q.strip()]
res = {"name": a.name, "cpu": subprocess.run("lscpu | sed -n 's/Model name: *//p'", shell=True, capture_output=True, text=True).stdout.strip(),
       "avx512": os.path.exists("/proc/cpuinfo") and "avx512f" in open("/proc/cpuinfo").read()}


def pct(lat):
    lat = sorted(lat)
    return {"p50_ms": round(statistics.median(lat), 2), "p95_ms": round(lat[int(0.95 * len(lat))], 2),
            "p99_ms": round(lat[int(0.99 * len(lat))], 2), "n": len(lat)}


if a.mode == "http":
    s = requests.Session()
    embed = lambda x: s.post(f"{a.a}/embed", json={"inputs": x, "truncate": True}, timeout=300).json()
    res["dims"] = len(embed(queries[0])[0])
    for q in queries[:30]:
        embed(q)
    lat = []
    for _ in range(3):
        for q in queries:
            t = time.perf_counter(); embed(q); lat.append((time.perf_counter() - t) * 1000)
    res["query"] = pct(lat)
    if a.container:
        res["mem"] = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", a.container],
                                    capture_output=True, text=True).stdout.strip()
    if a.ref:
        from sentence_transformers import SentenceTransformer
        m = SentenceTransformer(a.ref, device="cpu")
        ref = m.encode(queries[:50], normalize_embeddings=True)
        got = np.array([embed(q)[0] for q in queries[:50]], dtype=np.float32)
        got /= np.linalg.norm(got, axis=1, keepdims=True)
        res["cos_vs_st"] = round(float(np.mean(np.sum(ref * got, 1))), 5) if ref.shape == got.shape else f"{got.shape} vs {ref.shape}"
    if a.docs:  # synthetic docs with realistic length (~150 words, the production template is <=~120 words + fields)
        words = ("engineer team product customer data build manage support develop design senior remote experience "
                 "systems sales operations growth platform software services clients projects quality").split()
        random.seed(0)
        docs = [" ".join(random.choice(words) for _ in range(random.randint(90, 180))) for _ in range(1024)]
        batches = [docs[i:i + 32] for i in range(0, len(docs), 32)]
        t = time.perf_counter()
        with ThreadPoolExecutor(4) as pool:
            list(pool.map(embed, batches))
        res["docs_per_s"] = round(len(docs) / (time.perf_counter() - t), 1)
else:
    import onnxruntime as ort
    from tokenizers import Tokenizer
    tok = Tokenizer.from_file(f"{a.a}/tokenizer.json"); tok.enable_truncation(32)
    so = ort.SessionOptions(); so.intra_op_num_threads = a.threads; so.inter_op_num_threads = 1
    so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    sess = ort.InferenceSession(a.b, so, providers=["CPUExecutionProvider"])

    def run(q):
        e = tok.encode(q)
        ids = np.array([e.ids], dtype=np.int64)
        return sess.run(None, {"input_ids": ids, "attention_mask": np.ones_like(ids)})[0]
    for q in queries[:30]:
        run(q)
    lat = []
    for _ in range(5):
        for q in queries:
            t = time.perf_counter(); run(q); lat.append((time.perf_counter() - t) * 1000)
    res["query"] = pct(lat); res["threads"] = a.threads
    res["tokens_mean"] = round(float(np.mean([len(tok.encode(q).ids) for q in queries])), 1)
    import resource
    res["maxrss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
print(json.dumps(res))
open("results.jsonl", "a").write(json.dumps(res) + "\n")
if os.environ.get("GITHUB_STEP_SUMMARY"):
    open(os.environ["GITHUB_STEP_SUMMARY"], "a").write(f"- `{json.dumps(res)}`\n")
