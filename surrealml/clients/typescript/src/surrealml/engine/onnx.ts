/**
 * This adapter handles the ONNX file format.
 * The input model is already in ONNX format; this simply saves it to disk.
 * Kept for structural consistency with other engine adapters.
 */
import fs from 'fs';
import { createFileCachePath } from './utils.js';


export class OnnxAdapter {
    /**
     * Saves a model to an ONNX file.
     *
     * model: raw onnx protobuf bytes
     * returns: the path to the cache file (unique to prevent collisions)
     */
	static saveModelToOnnx(model: Uint8Array): string {
		const filePath = createFileCachePath();      
		fs.writeFileSync(filePath, model);  
		return filePath;
	  }	
}
