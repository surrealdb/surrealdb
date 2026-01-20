# SurrealML LLMs
## Running and Testing
- It's best to run this repo through the tests as right now it's a WIP for demonstration purposes.
- There are 2 feature flags to keep in mind. 'http-access' and 'local-gemma-test'. http-access will load a model over 
http whilst local-gemma-test expects the Gemma-7B model to already be loaded locally in your default hugging face cache
which is at ```~/.cache/huggingface/hub```. 
- The local-gemma-test feature flag really tests the full capacity of this crate, whilst with the http-access flag just tests loading
models over http.

So the best approach to testing is this - 
1. Clear your cache at ```~/.cache/huggingface/hub```.
2. If you go to ```llms/src/interface/load_model.rs``` and look at the ```load_model_via_http``` test, you'll see we call the load_model function with a dummy token. Paste your real hugging face token there instead.
3. Run ```cargo test --features http-access```. This will run all tests gated by this feature, including a test for loading the 
Gemma-7B model remotely from hugging face.
4. Now the files for the Gemma-7B model should be stored in ```~/.cache/huggingface/hub```. Now we can run ```cargo test --features local-gemma-test```. This will run the full spectrum of functionality for the library, including loading the Gemma-7B model from 
your local hugging face cache and running it using the tokenizers crate. 

## Testing Limitations to change ASAP
- Having to change the hugging face token manually needs to be changed to a CLI update.
- In ```llms/src/tensors/fetch_tensors.rs``` we have the ```download_and_verify_gemma7b_safetensors``` test which is similar to our ```load_model_via_http``` test talked about in the above section. Both are gated by http-acces so both would run when that feature is enabled, which is costly time wise. So for now we've commented out ```download_and_verify_gemma7b_safetensors``` as we will refactor this later. ```download_and_verify_gemma7b_safetensors``` has the same issue as ```load_model_via_http``` in that you need to hardcode a hugging face token.

## Future Improvements
This README will be formalised later, but for now a running list of future improvements or notes - 
- We use 2 features, 1 for functionality and testing, one just for testing.
- When pulling from hugging face hub we could do a lazy static API, and we could do a custom cache path.
- We aren't supporting CUDA out of the both. This means for each model, the flash_attn boolean is defaulted to false - both when creating the model config and when creating the actual model object. We don't use the CUDA feature in our candle-transformers crate either. We also choose device type CPU both when creating the VarBuilder object and when running the model.
- We only test the model loading methods in each model file with the gemma feature flag for just Gemma. We can look into doing more expansive tests later.
- Store Gemma config.jsons in the binary to support different ones. Right now that's hardcoded.
- Remove need for enum with boxed dyn trait calling?
- Refactor running the models with traits to make dependency injection.
- Remove state, and hold loaded on the struct itself
