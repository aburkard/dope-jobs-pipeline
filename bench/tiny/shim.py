"""Minimal TEI-compatible ONNX Runtime embedding server: POST /embed {"inputs": str|[str]} -> [[float]].

usage: shim.py MODEL_DIR ONNX_FILE PORT [THREADS=4]
"""
import json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import numpy as np, onnxruntime as ort
from tokenizers import Tokenizer

d, onnx_file, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
threads = int(sys.argv[4]) if len(sys.argv) > 4 else 4
tok = Tokenizer.from_file(f"{d}/tokenizer.json")
tok.enable_truncation(32)
so = ort.SessionOptions(); so.intra_op_num_threads = threads; so.inter_op_num_threads = 1
so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
sess = ort.InferenceSession(onnx_file, so, providers=["CPUExecutionProvider"])


def embed(texts):
    encs = tok.encode_batch(texts)
    n = max(len(e.ids) for e in encs)
    ids = np.zeros((len(encs), n), dtype=np.int64); mask = np.zeros_like(ids)
    for i, e in enumerate(encs):
        ids[i, :len(e.ids)] = e.ids; mask[i, :len(e.ids)] = 1
    return sess.run(None, {"input_ids": ids, "attention_mask": mask})[0]


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"ok")

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        x = body.get("inputs") or body.get("input")
        out = json.dumps(embed([x] if isinstance(x, str) else x).tolist()).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out))); self.end_headers(); self.wfile.write(out)


embed(["warmup"])
ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
