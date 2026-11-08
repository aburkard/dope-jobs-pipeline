"""voyage-4-nano single-query CPU serving benchmark (throwaway; no secrets, public models only).

Every variant reports cosine vs the sentence-transformers fp32 reference over the first 30 queries
(512-dim Matryoshka slice + L2 normalize), single-query p50/p95 at 4 threads, and RSS.

usage:
  nano_bench.py ref                                  # write /tmp/nano_ref.npy (sentence-transformers fp32, 512d)
  nano_bench.py http NAME URL tei|llama [--pid PID]  # running TEI (/embed) or llama-server (/v1/embeddings)
  nano_bench.py ort NAME MODEL.onnx                  # ONNX Runtime CPU, 4 threads
  nano_bench.py export OUT_DIR                       # own ONNX export (explicit bidirectional mask) + int8 variants
  nano_bench.py torch NAME fp32|int8                 # sentence-transformers, optionally torch dynamic int8 Linear
"""
import json, os, resource, statistics, sys, time
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
queries = [q.strip() for q in open(f"{HERE}/queries.txt") if q.strip()]
PROMPT = "Represent the query for retrieving supporting documents: "
REPO = "voyageai/voyage-4-nano"
REF = "/tmp/nano_ref.npy"
DIM = 512
ISA = {k: os.environ[k] for k in ("ATEN_CPU_CAPABILITY", "MKL_ENABLE_INSTRUCTIONS", "ONEDNN_MAX_CPU_ISA",
                                  "FBGEMM_ENABLE_INSTRUCTIONS") if k in os.environ}


def norm512(v):
    v = np.asarray(v, dtype=np.float32).reshape(-1)[:DIM]
    return v / np.linalg.norm(v)


def timed(fn):
    for q in queries[:10]:
        fn(q)
    lat = []
    for _ in range(3):
        for q in queries:
            t = time.perf_counter(); fn(q); lat.append((time.perf_counter() - t) * 1000)
    lat.sort()
    return {"p50_ms": round(statistics.median(lat), 1), "p95_ms": round(lat[int(0.95 * len(lat))], 1),
            "max_ms": round(lat[-1], 1)}


def cos(fn):
    ref = np.load(REF)
    out = np.stack([norm512(fn(q)) for q in queries[:30]])
    c = np.sum(out * ref, axis=1)
    return {"cos_mean": round(float(c.mean()), 5), "cos_min": round(float(c.min()), 5)}


def rss(pid="self"):
    st = dict(l.split(":", 1) for l in open(f"/proc/{pid}/status") if ":" in l)
    return {"rss_mb": int(st["VmRSS"].split()[0]) // 1024, "peak_rss_mb": int(st["VmHWM"].split()[0]) // 1024}


def report(res):
    res["isa_env"] = ISA or "native"
    print(json.dumps(res))
    with open(os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null"), "a") as f:
        f.write(f"### {res['name']}\n```\n{json.dumps(res, indent=1)}\n```\n")


def st_model():
    import torch
    from sentence_transformers import SentenceTransformer
    torch.set_num_threads(4)
    return SentenceTransformer(REPO, device="cpu", trust_remote_code=True, truncate_dim=DIM)


def main():
    mode = sys.argv[1]
    if mode == "ref":
        m = st_model()
        ref = m.encode([PROMPT + q for q in queries[:30]], normalize_embeddings=True)
        np.save(REF, ref.astype(np.float32)); print("ref", ref.shape)
        return
    name = sys.argv[2]
    res = {"name": name}
    if mode == "http":
        import requests
        url, api = sys.argv[3], sys.argv[4]
        s = requests.Session()
        def enc(q):
            if api == "tei":
                r = s.post(f"{url}/embed", json={"inputs": PROMPT + q, "truncate": True}, timeout=120)
                r.raise_for_status(); return r.json()[0]
            r = s.post(f"{url}/v1/embeddings", json={"input": PROMPT + q}, timeout=120)
            r.raise_for_status(); return r.json()["data"][0]["embedding"]
        res["dims"] = len(enc(queries[0]))
        res.update(cos(enc)); res.update(timed(enc))
        if "--pid" in sys.argv:
            res.update(rss(sys.argv[sys.argv.index("--pid") + 1]))
    elif mode == "ort":
        import onnxruntime as ort
        from transformers import AutoTokenizer
        path = sys.argv[3]
        tok = AutoTokenizer.from_pretrained(REPO)
        so = ort.SessionOptions(); so.intra_op_num_threads = 4; so.inter_op_num_threads = 1
        t0 = time.time()
        sess = ort.InferenceSession(path, so, providers=["CPUExecutionProvider"])
        res["load_s"] = round(time.time() - t0, 1)
        ins = [i.name for i in sess.get_inputs()]
        outs = [o.name for o in sess.get_outputs()]
        res["inputs"], res["outputs"] = ins, outs
        d = os.path.dirname(path); base = os.path.basename(path)
        res["file_mb"] = round(sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d) if f.startswith(base)) / 1e6)
        def enc(q):
            t = tok([PROMPT + q], return_tensors="np")
            ids = t["input_ids"].astype(np.int64); am = t["attention_mask"].astype(np.int64)
            feed = {"input_ids": ids, "attention_mask": am}
            if "position_ids" in ins:
                feed["position_ids"] = np.arange(ids.shape[1], dtype=np.int64)[None]
            o = dict(zip(outs, sess.run(None, feed)))
            if "embedding" in o:  # own export: pooled/sliced/normalized in-graph
                return o["embedding"][0]
            h = o["last_hidden_state"][0]  # (seq, 2048), already projected; mean pool over tokens
            return (h * am[0][:, None]).sum(0) / am.sum()
        res.update(cos(enc)); res.update(timed(enc))
        res.update(rss())
    elif mode == "export":
        export(sys.argv[2])
        return
    elif mode == "torch":
        import torch
        kind = sys.argv[3]
        m = st_model()
        if kind == "int8":
            torch.backends.quantized.engine = "x86" if "x86" in torch.backends.quantized.supported_engines else "fbgemm"
            res["qengine"] = torch.backends.quantized.engine
            m[0].auto_model = torch.ao.quantization.quantize_dynamic(m[0].auto_model, {torch.nn.Linear}, dtype=torch.qint8)
        enc = lambda q: m.encode([PROMPT + q])[0]
        res.update(cos(enc)); res.update(timed(enc))
        res.update(rss())
    report(res)


def export(out_dir):
    """Export Qwen3 backbone with an explicit additive bidirectional mask (bypasses the custom vmap mask function
    that broke the legacy exporter); mean pool -> linear[:512] -> L2 baked into the graph."""
    import torch
    from onnxruntime.quantization import QuantType, quantize_dynamic
    m = st_model()
    tok = m.tokenizer
    base = m[0].auto_model.eval()
    os.makedirs(out_dir, exist_ok=True)

    class Wrap(torch.nn.Module):
        def __init__(self, b):
            super().__init__(); self.backbone = b.model; self.w = torch.nn.Parameter(b.linear.weight[:DIM].detach().clone())
        def forward(self, input_ids, attention_mask):
            am = attention_mask.to(torch.float32)
            add = (1.0 - am)[:, None, None, :] * torch.finfo(torch.float32).min
            add = add.expand(-1, 1, input_ids.shape[1], -1)
            h = self.backbone(input_ids=input_ids, attention_mask={"full_attention": add}, use_cache=False).last_hidden_state
            pooled = (h * am[..., None]).sum(1) / am.sum(1, keepdim=True).clamp(min=1e-9)
            e = pooled @ self.w.T  # linear has no bias, so projecting after mean pool == before
            return e / e.norm(dim=-1, keepdim=True).clamp(min=1e-12)

    w = Wrap(base).eval()
    sample = tok([PROMPT + "senior backend engineer", PROMPT + "registered nurse night shift"], padding=True, return_tensors="pt")
    with torch.no_grad():  # sanity: wrapper matches sentence-transformers before exporting
        ref = m.encode([PROMPT + "senior backend engineer"], normalize_embeddings=True)[0]
        one = tok([PROMPT + "senior backend engineer"], return_tensors="pt")
        got = w(one["input_ids"], one["attention_mask"])[0].numpy()
        print("wrapper cos vs st:", float(np.dot(ref, got)))
    path = f"{out_dir}/model.onnx"
    errs = {}
    for dynamo in (False, True):
        try:
            t0 = time.time()
            kw = dict(input_names=["input_ids", "attention_mask"], output_names=["embedding"])
            if dynamo:
                B, S = torch.export.Dim("b"), torch.export.Dim("s", min=2, max=8192)
                torch.onnx.export(w, (sample["input_ids"], sample["attention_mask"]), path, dynamo=True, external_data=True,
                                  dynamic_shapes={"input_ids": {0: B, 1: S}, "attention_mask": {0: B, 1: S}}, **kw)
            else:
                torch.onnx.export(w, (sample["input_ids"], sample["attention_mask"]), path, opset_version=17, dynamo=False,
                                  dynamic_axes={"input_ids": {0: "b", 1: "s"}, "attention_mask": {0: "b", 1: "s"},
                                                "embedding": {0: "b"}}, **kw)
            print(f"export ok dynamo={dynamo} in {time.time() - t0:.0f}s"); break
        except Exception as e:  # noqa: BLE001
            errs[dynamo] = f"{type(e).__name__}: {str(e)[:300]}"; print("export failed", dynamo, errs[dynamo])
    else:
        raise SystemExit(f"all exports failed: {errs}")
    print(os.listdir(out_dir))
    quantize_dynamic(path, f"{out_dir}/model_int8.onnx", weight_type=QuantType.QInt8)
    quantize_dynamic(path, f"{out_dir}/model_int8rr.onnx", weight_type=QuantType.QInt8, per_channel=True, reduce_range=True)
    print(os.listdir(out_dir))


if __name__ == "__main__":
    main()
