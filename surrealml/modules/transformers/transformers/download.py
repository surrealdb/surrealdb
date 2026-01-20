from huggingface_hub import hf_hub_download
repo  = "Tech-oriented/bert-base-uncased-finetuned-sst2"
path  = hf_hub_download(repo, "model.safetensors")   # auto‑uses the env token
cfg   = hf_hub_download(repo, "config.json")
print("weights →", path)
print("config  →", cfg)