"""voyage-4-nano query serving on CPU: torch (optionally AVX2-restricted via env) and an ONNX export under ORT.

usage: nano_onnx.py torch_nano | torch_pplx | onnx
  torch_*: sentence-transformers fp32, 4 threads; run with ATEN_CPU_CAPABILITY=avx2 etc. to emulate the box's AVX2-only CPU
  onnx:    export nano (mean pooling + L2 norm + 512d MRL slice baked in) to ONNX, check parity vs sentence-transformers,
           then ORT fp32 and dynamic-int8 latency at 4 threads
"""
import json, os, resource, statistics, sys, time
import numpy as np
import torch

variant = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))
queries = [q.strip() for q in open(f"{HERE}/queries.txt") if q.strip()]
PROMPT = "Represent the query for retrieving supporting documents: "
torch.set_num_threads(4)
res = {"name": f"{variant}", "isa_env": os.environ.get("ATEN_CPU_CAPABILITY", "default")}


def timed(fn):
    for q in queries[:10]:
        fn(q)
    lat = []
    for _ in range(3):
        for q in queries:
            t = time.perf_counter(); fn(q); lat.append((time.perf_counter() - t) * 1000)
    lat.sort()
    return {"query_p50_ms": round(statistics.median(lat), 1), "query_p95_ms": round(lat[int(0.95 * len(lat))], 1)}


from sentence_transformers import SentenceTransformer
if variant == "torch_pplx":
    m = SentenceTransformer("perplexity-ai/pplx-embed-v1-0.6B", device="cpu", trust_remote_code=True)
    res.update(timed(lambda q: m.encode([q])))
elif variant == "torch_nano":
    m = SentenceTransformer("voyageai/voyage-4-nano", device="cpu", trust_remote_code=True, truncate_dim=512)
    res.update(timed(lambda q: m.encode([q], prompt=PROMPT)))
else:
    import onnxruntime as ort
    from onnxruntime.quantization import QuantType, quantize_dynamic
    st = SentenceTransformer("voyageai/voyage-4-nano", device="cpu", trust_remote_code=True, truncate_dim=512)
    tok = st.tokenizer
    base = st[0].auto_model.eval()

    class Wrap(torch.nn.Module):
        def __init__(self, m):
            super().__init__(); self.m = m
        def forward(self, input_ids, attention_mask):
            h = self.m(input_ids=input_ids, attention_mask=attention_mask).last_hidden_state
            mask = attention_mask.unsqueeze(-1).to(h.dtype)
            pooled = (h * mask).sum(1) / mask.sum(1).clamp(min=1e-9)
            pooled = pooled[:, :512]
            return pooled / pooled.norm(dim=-1, keepdim=True).clamp(min=1e-12)

    sample = tok([PROMPT + "senior backend engineer", PROMPT + "nurse"], padding=True, return_tensors="pt")
    os.makedirs("/tmp/nano-onnx", exist_ok=True)
    path = "/tmp/nano-onnx/model.onnx"
    t0 = time.time()
    torch.onnx.export(Wrap(base), (sample["input_ids"], sample["attention_mask"]), path, opset_version=17, dynamo=False,
                      input_names=["input_ids", "attention_mask"], output_names=["embedding"],
                      dynamic_axes={"input_ids": {0: "b", 1: "s"}, "attention_mask": {0: "b", 1: "s"}, "embedding": {0: "b"}})
    res["export_s"] = round(time.time() - t0, 1)
    ref = st.encode([PROMPT + q for q in queries[:30]], normalize_embeddings=True, prompt=None)
    for kind in ("fp32", "int8"):
        p = path
        if kind == "int8":
            p = "/tmp/nano-onnx/model_int8.onnx"
            quantize_dynamic(path, p, weight_type=QuantType.QInt8)
        so = ort.SessionOptions(); so.intra_op_num_threads = 4; so.inter_op_num_threads = 1
        sess = ort.InferenceSession(p, so, providers=["CPUExecutionProvider"])
        def enc(q, sess=sess):
            t = tok([PROMPT + q], return_tensors="np")
            return sess.run(["embedding"], {"input_ids": t["input_ids"].astype(np.int64), "attention_mask": t["attention_mask"].astype(np.int64)})[0]
        out = np.concatenate([enc(q) for q in queries[:30]])
        res[f"ort_{kind}_cos_vs_st"] = round(float(np.mean(np.sum(out * ref, axis=1))), 5)
        res[f"ort_{kind}_size_mb"] = round(sum(os.path.getsize(os.path.join("/tmp/nano-onnx", f)) for f in os.listdir("/tmp/nano-onnx")
                                               if f.startswith(os.path.basename(p).split(".")[0])) / 1e6)
        res.update({f"ort_{kind}_{k}": v for k, v in timed(enc).items()})
res["peak_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
print(json.dumps(res))
with open(os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null"), "a") as f:
    f.write(f"### {res['name']}\n```\n{json.dumps(res, indent=1)}\n```\n")
