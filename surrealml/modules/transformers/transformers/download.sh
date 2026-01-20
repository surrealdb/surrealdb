repo="Xenova/bert-base-uncased-finetuned-sst2"
dest="modules/transformers"

# config (~700 bytes)
curl -L -o ./config_general.json \
     "https://huggingface.co/${repo}/resolve/main/config.json?download=true"

# weights (~420 MB – this is the big one; be patient)
curl -L -o ./model_general.safetensors \
     "https://huggingface.co/${repo}/resolve/main/model.safetensors?download=true"