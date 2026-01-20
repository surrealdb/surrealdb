from transformers import AutoConfig, AutoModelForSequenceClassification
from safetensors.torch import save_file
import torch

repo = "textattack/bert-base-uncased-SST-2"          # or any other
model = AutoModelForSequenceClassification.from_pretrained(repo,
          trust_remote_code=False)
cfg    = model.config

# 1. save weights
state_dict = model.state_dict()
save_file(state_dict, "model.safetensors")           # ~420 MB

# 2. save cfg in Candle/transformers JSON format
#    (Candle's `Config` mirrors HF's, just rename keys if needed)
import json, os
with open("config.json", "w") as f:
    json.dump(cfg.to_dict(), f, indent=2)
print("wrote model.safetensors & config.json")
