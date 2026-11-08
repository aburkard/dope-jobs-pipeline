"""Build a random-weight student with the exact shapes of the trained one (latency/RSS do not depend on weights).

Throwaway benchmark: no secrets, no production data. Uses the public multilingual-e5-small tokenizer, trimmed to
VOCAB pieces by unigram score (the trained model keeps the pieces its corpus uses; same size, same structure).
Writes a sentence-transformers folder (Transformer -> mean Pooling -> Dense 384->256 no bias -> Normalize) that TEI
serves natively, plus ONNX (full pipeline incl. pooling/dense/normalize) fp32 and dynamic int8.

usage: build.py OUT_DIR LAYERS [VOCAB=100007] [DIMS=256]
"""
import json, os, sys
import torch, torch.nn as nn, torch.nn.functional as F
from huggingface_hub import hf_hub_download
from transformers import BertConfig, BertModel, AutoTokenizer

out, L = sys.argv[1], int(sys.argv[2])
V = int(sys.argv[3]) if len(sys.argv) > 3 else 100007
DIMS = int(sys.argv[4]) if len(sys.argv) > 4 else 256
torch.manual_seed(0)
base = "intfloat/multilingual-e5-small"
tj = json.load(open(hf_hub_download(base, "tokenizer.json")))
vocab = tj["model"]["vocab"]
keep = set(range(4)) | {i for i, (p, _) in enumerate(vocab) if len(p.replace("▁", "")) <= 1}
for i in sorted(range(4, len(vocab)), key=lambda i: -vocab[i][1]):
    if len(keep) >= V - 1:
        break
    keep.add(i)
keep = sorted(keep)
tj["model"]["vocab"] = [vocab[i] for i in keep]
for a in tj["added_tokens"]:
    if a["id"] not in range(4):
        a["id"] = len(keep)
cfg = BertConfig.from_pretrained(base)
cfg.vocab_size, cfg.num_hidden_layers = len(keep) + 1, L
enc = BertModel(cfg).eval()
os.makedirs(out, exist_ok=True)
enc.save_pretrained(out)
AutoTokenizer.from_pretrained(base).save_pretrained(out)
json.dump(tj, open(f"{out}/tokenizer.json", "w"), ensure_ascii=False)
if os.path.exists(f"{out}/sentencepiece.bpe.model"):
    os.remove(f"{out}/sentencepiece.bpe.model")
H = cfg.hidden_size
dense = nn.Linear(H, DIMS, bias=False)
os.makedirs(f"{out}/1_Pooling", exist_ok=True); os.makedirs(f"{out}/2_Dense", exist_ok=True)
json.dump({"word_embedding_dimension": H, "pooling_mode_cls_token": False, "pooling_mode_mean_tokens": True,
           "pooling_mode_max_tokens": False, "pooling_mode_mean_sqrt_len_tokens": False}, open(f"{out}/1_Pooling/config.json", "w"))
json.dump({"in_features": H, "out_features": DIMS, "bias": False,
           "activation_function": "torch.nn.modules.linear.Identity"}, open(f"{out}/2_Dense/config.json", "w"))
from safetensors.torch import save_file
save_file({"linear.weight": dense.weight.data.contiguous()}, f"{out}/2_Dense/model.safetensors")
json.dump([{"idx": 0, "name": "0", "path": "", "type": "sentence_transformers.models.Transformer"},
           {"idx": 1, "name": "1", "path": "1_Pooling", "type": "sentence_transformers.models.Pooling"},
           {"idx": 2, "name": "2", "path": "2_Dense", "type": "sentence_transformers.models.Dense"},
           {"idx": 3, "name": "3", "path": "3_Normalize", "type": "sentence_transformers.models.Normalize"}],
          open(f"{out}/modules.json", "w"))
json.dump({"max_seq_length": 32, "do_lower_case": False}, open(f"{out}/sentence_bert_config.json", "w"))


class Full(nn.Module):
    def __init__(s):
        super().__init__(); s.enc, s.dense = enc, dense

    def forward(s, input_ids, attention_mask):
        h = s.enc(input_ids=input_ids, attention_mask=attention_mask).last_hidden_state
        m = attention_mask.unsqueeze(-1).to(h.dtype)
        return F.normalize(s.dense((h * m).sum(1) / m.sum(1).clamp(min=1)), dim=-1)


full = Full().eval()
ids = torch.ones(1, 8, dtype=torch.long)
os.makedirs(f"{out}_onnx", exist_ok=True)
torch.onnx.export(full, (ids, torch.ones_like(ids)), f"{out}_onnx/model.onnx", input_names=["input_ids", "attention_mask"],
                  output_names=["embedding"], dynamic_axes={"input_ids": {0: "b", 1: "s"}, "attention_mask": {0: "b", 1: "s"},
                                                            "embedding": {0: "b"}}, opset_version=17, dynamo=False)
from onnxruntime.quantization import quantize_dynamic, QuantType
quantize_dynamic(f"{out}_onnx/model.onnx", f"{out}_onnx/model_int8.onnx", weight_type=QuantType.QInt8, per_channel=True)
print(out, "layers", L, "vocab", cfg.vocab_size, "params %.1fM" % (sum(p.numel() for p in enc.parameters()) / 1e6),
      {f: round(os.path.getsize(f) / 1e6, 1) for f in (f"{out}/model.safetensors", f"{out}_onnx/model.onnx", f"{out}_onnx/model_int8.onnx")})
