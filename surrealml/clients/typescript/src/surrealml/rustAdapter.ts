import { EmptyReturn, StringReturn, Vecf32Return, FileInfo, VecU8Return } from './cStructs.js';
import type { EmptyReturnType, StringReturnType, Vecf32ReturnType, FileInfoType, VecU8ReturnType } from './cStructs.js';
import { Engine } from './engine/engine.js';
import { LibLoader } from './loader.js';
import { Buffer } from 'buffer';
import koffi from 'koffi';


/**
 * The adapter to interact with the Rust module compiled to a C dynamic library
 */
export class RustAdapter {
	/**
	 * fileId: the unique id of the model in the Rust library
	 * engine: the engine used to load and save the model
	 */
	constructor(fileId: string, engine: Engine) {
        this.fileId = fileId;
        this.engine = engine;
		this.loader = LibLoader.getInstance();
	}
    
    private fileId: string;
    private engine: Engine;
	private loader: LibLoader;

	/**
	 * Points to a raw ONNX file and passes it into the rust library so it can be loaded
	 * and tagged with a unique id so the Rust library can reference this model again
	 * from within the rust library.
	 *
	 * filePath: the path to the raw ONNX file.
	 *
	 * returns: the unique id of the model.
	 */
	static passRawModelIntoRust(filePath: string): string {
		const loader = LibLoader.getInstance();
		const out = loader.lib.load_cached_raw_model(filePath) as StringReturnType;

		if (out.is_error == 1) throw new Error(out.error_message || 'Unknown error whilst parsing model into Rust.');
		return out.string;
	}

	/**
	 * Loads a model from a file.
	 *
	 * path: the path to load the model from.
	 *
	 * returns: [fileId, name, description, version]
	 */
	static load(path: string): [string, string, string, string] {
		const loader = LibLoader.getInstance();
		const out = loader.lib.load_model(path) as FileInfoType;
		if (out.is_error == 1) throw new Error(out.error_message || 'Unknown error whilst loading model.');

		const ret: [string, string, string, string] = [
			out.file_id,
			out.name,
			out.description,
			out.version
		];
		return ret;
	}

	/**
	 * Uploads a model to a remote server.
	 *
	 * path: the path to load the model from.
	 * url: the url of the remote server.
	 * chunkSize: the size of each chunk to upload.
	 * namespace: the namespace of the remote server.
	 * database: the database of the remote server.
	 * username: the username of the remote server.
	 * password: the password of the remote server.
	 *
	 * returns: none
	 */
	static upload(
		path: string,
		url: string,
		chunkSize: number,
		namespace: string,
		database: string,
		username: string | null = null,
		password: string | null = null
	): void {
		const loader = LibLoader.getInstance();
	
		const out = loader.lib.upload_model(
			path,
			url,
			chunkSize,
			namespace,
			database,
			username ?? '',
			password ?? ''
		) as EmptyReturnType;
	
		if (out.is_error === 1) {
			throw new Error(out.error_message || 'Unknown error whilst uploading model');
		}
	}


	/**
	 * Adds a column to the model metadata (order matters).
	 *
	 * name: the name of the column.
	 *
	 * returns: none
	 */
	addColumn(name: string): void {
		const out = this.loader.lib.add_column(this.fileId, name) as EmptyReturnType;
	
		if (out.is_error === 1) {
			throw new Error(out.error_message || 'Unknown error whilst adding column.');
		}
	}

	/**
	 * Adds an output to the model metadata.
	 *
	 * outputName: the name of the output.
	 * normaliserType: the type of normaliser to use.
	 * one: the first parameter of the normaliser.
	 * two: the second parameter of the normaliser.
	 *
	 * returns: none
	 */
	addOutput(
		outputName: string,
		normaliserType: string,
		one: number,
		two: number
	): void {
		const out = this.loader.lib.add_output(
			this.fileId,
			outputName,
			normaliserType,
			one.toString(),
			two.toString()
		) as EmptyReturnType;
	
		if (out.is_error === 1) {
			throw new Error(out.error_message || 'Unknown error whilst adding output.');
		}
	}

	/**
	 * Adds a description to the model metadata.
	 *
	 * description: the description of the model.
	 *
	 * returns: none
	 */
	addDescription(description: string): void {
		const out = this.loader.lib.add_description(
			this.fileId,
			description
		) as EmptyReturnType;
	
		if (out.is_error === 1) {
			throw new Error(out.error_message || 'Unknown error whilst adding description.');
		}
	}

	/**
	 * Adds a version to the model metadata.
	 *
	 * version: the version of the model.
	 *
	 * returns: none
	 */
	addVersion(version: string): void {
		const out = this.loader.lib.add_version(
			this.fileId,
			version
		) as EmptyReturnType;
	
		if (out.is_error === 1) {
			throw new Error(out.error_message || 'Unknown error whilst adding version.');
		}
	}

	/**
	 * Adds a name to the model metadata.
	 *
	 * name: the name of the model.
	 *
	 * returns: none
	 */
	addName(name: string): void {
		const out = this.loader.lib.add_name(
			this.fileId,
			name
		) as EmptyReturnType;
	
		if (out.is_error === 1) {
			throw new Error(out.error_message || 'Unknown error whilst adding name.');
		}
	}

	/**
	 * Adds a normaliser to the model metadata for a column.
	 *
	 * columnName: the name of the column.
	 * normaliserType: the type of normaliser to use.
	 * one: the first parameter of the normaliser.
	 * two: the second parameter of the normaliser.
	 *
	 * returns: none
	 */
	addNormaliser(
		columnName: string,
		normaliserType: string,
		one: number,
		two: number
	): void {
		const out = this.loader.lib.add_normaliser(
			this.fileId,
			columnName,
			normaliserType,
			one.toString(),
			two.toString()
		) as EmptyReturnType;

		if (out.is_error === 1) {
			throw new Error(out.error_message || 'Unknown error whilst adding normaliser.');
		}
	}

	/**
	 * Adds an author to the model metadata.
	 *
	 * author: the author of the model.
	 *
	 * returns: none
	 */
	addAuthor(author: string): void {
		const out = this.loader.lib.add_author(
			this.fileId,
			author
		) as EmptyReturnType;
	
		if (out.is_error === 1) {
			throw new Error(out.error_message || 'Unknown error whilst adding author.');
		}
	}

	/**
	 * Saves the model to a file.
	 *
	 * path: the path to save the model to.
	 * name: the name of the model.
	 *
	 * returns: none
	 */
	save(path: string, name: string | null = null): void {
		/* engine ---------------------------------------------------------------- */
		let out = this.loader.lib.add_engine(
			this.fileId,
			String(this.engine)
		) as EmptyReturnType;
		if (out.is_error === 1) throw new Error(out.error_message || 'Unknown error whilst adding engine.');
	
		/* origin ---------------------------------------------------------------- */
		out = this.loader.lib.add_origin(
			this.fileId,
			'local'
		) as EmptyReturnType;
		if (out.is_error === 1) throw new Error(out.error_message || 'Unknown error whilst adding origin.');
	
		/* optional name --------------------------------------------------------- */
		if (name !== null) {
			out = this.loader.lib.add_name(
				this.fileId,
				name
			) as EmptyReturnType;
			if (out.is_error === 1) throw new Error(out.error_message || 'Unknown error whilst adding name.');
		} else {
			console.warn('Saving model without a name – upload to DB will be blocked.');
		}
	
		/* final write ----------------------------------------------------------- */
		out = this.loader.lib.save_model(
			path,
			this.fileId
		) as EmptyReturnType;
		if (out.is_error === 1) throw new Error(out.error_message || 'Unknown error whilst saving model.');
	}

	/**
	 * Converts the model to bytes.
	 *
	 * returns: the model as bytes.
	 */
	toBytes(): Uint8Array {
		const out = this.loader.lib.to_bytes(this.fileId) as VecU8ReturnType;
		if (out.is_error === 1) {
		  throw new Error(out.error_message || '...');
		}

		const buf = koffi.view(out.data, out.length);       // ArrayBuffer
		return new Uint8Array(buf);
	}

	/**
	 * Calculates an output from the model given an input vector.
	 *
	 * inputVector: a 1D vector of inputs to the model.
	 * dims: the dimensions of the input vector to be sliced into.
	 *
	 * returns: the output of the model.
	 */
	rawCompute(inputVector: number[]): number[] {
		const out = this.loader.lib.raw_compute(
			this.fileId,
			new Float32Array(inputVector),
			inputVector.length
		) as Vecf32ReturnType;
	
		if (out.is_error === 1) {
			throw new Error(out.error_message || 'Unknown error whilst computing model.');
		}
	
		// length in bytes = number of floats * 4
		const byteLen = out.length * Float32Array.BYTES_PER_ELEMENT; 
		const buf = koffi.view(out.data, byteLen);              // now an ArrayBuffer of N*4 bytes
		const floats = new Float32Array(buf);                      // a Float32Array of length `out.length`

		return Array.from(floats);                                 // [f0, f1, …]
	}

	/**
	 * Calculates an output from the model given a value map.
	 *
	 * valueMap: a dictionary of inputs to the model with the column names as keys and floats as values.
	 *
	 * returns: the output of the model.
	 */
	bufferedCompute(valueMap: Record<string, number>): number[] {
		const keys = Object.keys(valueMap);
		const values = keys.map(k => {
			const v = valueMap[k];
			if (typeof v !== 'number') {
				throw new Error(`Value for key "${k}" is not a number`);
			}
			return v;
		});
	  
		const out = this.loader.lib.buffered_compute(
			this.fileId,              // char *
			new Float32Array(values), // float *
			values.length,            // size_t
			keys,                     // char **  (string[])
			keys.length               // int
		) as Vecf32ReturnType;
	  
		if (out.is_error === 1) {
		  	throw new Error(out.error_message ?? 'buffered_compute failed');
		};
	  
		// length in bytes = number of floats * 4
		const byteLen = out.length * Float32Array.BYTES_PER_ELEMENT; 
		const buf = koffi.view(out.data, byteLen);             
		const floats = new Float32Array(buf);                      

		return Array.from(floats);                               
	}	  
}