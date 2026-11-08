"""In-process CPU benchmarks on the runner (throwaway; no secrets).

- pplx-embed-v1-0.6B ONNX fp32 vs int8 (model_quantized.onnx): single-query latency, peak RSS (one process each)
- mdbr-leaf-ir via sentence-transformers torch: single-query latency and document throughput
  (the path a GitHub Actions load job would use to embed documents)

usage: bench_py.py onnx_fp32|onnx_int8|leaf_torch
"""
import json, os, random, resource, statistics, sys, time

variant = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))
queries = [q.strip() for q in open(f"{HERE}/queries.txt") if q.strip()]
res = {"name": f"py_{variant}"}


def timed(fn, prefix=""):
    for q in queries[:10]:
        fn(prefix + q)
    lat = []
    for _ in range(3):
        for q in queries:
            t = time.perf_counter(); fn(prefix + q); lat.append((time.perf_counter() - t) * 1000)
    lat.sort()
    return {"query_p50_ms": round(statistics.median(lat), 1), "query_p95_ms": round(lat[int(0.95 * len(lat))], 1)}


if variant.startswith("onnx"):
    import numpy as np, onnxruntime as ort
    from huggingface_hub import snapshot_download
    from transformers import AutoTokenizer
    repo = "perplexity-ai/pplx-embed-v1-0.6B"
    f = "onnx/model.onnx" if variant == "onnx_fp32" else "onnx/model_quantized.onnx"
    # local_dir: real files, so ONNX Runtime accepts the external-data paths (cache symlinks are rejected)
    local = snapshot_download(repo, allow_patterns=[f + "*", "*.json", "*.txt"], local_dir=f"/tmp/pplx-{variant}")
    path = f"{local}/{f}"
    so = ort.SessionOptions(); so.intra_op_num_threads = 4; so.inter_op_num_threads = 1
    sess = ort.InferenceSession(path, so, providers=["CPUExecutionProvider"])
    tok = AutoTokenizer.from_pretrained(repo, trust_remote_code=False)
    def enc(q):
        t = tok([q], return_tensors="np")
        return sess.run(["pooler_output_int8"], {"input_ids": t["input_ids"].astype(np.int64),
                                                 "attention_mask": t["attention_mask"].astype(np.int64)})
    res.update(timed(enc))
elif variant.startswith("nano"):
    import torch
    from sentence_transformers import SentenceTransformer
    torch.set_num_threads(4)
    kw = {"model_kwargs": {"torch_dtype": torch.bfloat16}} if variant == "nano_bf16" else {}
    m = SentenceTransformer("voyageai/voyage-4-nano", device="cpu", trust_remote_code=True, truncate_dim=512, **kw)
    res.update(timed(lambda q: m.encode([q], prompt="Represent the query for retrieving supporting documents: ")))
else:
    import torch
    from sentence_transformers import SentenceTransformer
    torch.set_num_threads(4)
    m = SentenceTransformer("MongoDB/mdbr-leaf-ir", device="cpu")
    res.update(timed(lambda q: m.encode([q], prompt_name="query")))
    words = "engineer team product customer data build manage support develop design senior remote experience " \
            "systems sales operations growth platform software services clients projects quality".split()
    random.seed(0)
    lengths = json.load(open(f"{HERE}/doc_word_counts.json"))
    docs = [" ".join(random.choice(words) for _ in range(n)) for n in lengths]
    t = time.perf_counter()
    m.encode(docs, batch_size=64)
    res["docs_per_s_torch_4threads"] = round(len(docs) / (time.perf_counter() - t), 1)
res["peak_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)  # Linux: KB
print(json.dumps(res))
with open(os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null"), "a") as f:
    f.write(f"### {res['name']}\n```\n{json.dumps(res, indent=1)}\n```\n")
