/**
 * Defines the SurMlFile class which is used to save/load models
 * and perform computations based on those models.
 */
import { Engine } from './engine/engine.js';
import { OnnxAdapter } from './engine/onnx.js';
import { RustAdapter } from './rustAdapter.js';

export class SurMlFile {
	model: any;
	name: string | null;
	engine: Engine | null;
	fileId: string | null;
	rustAdapter: RustAdapter;
	description: string | null = null;
	version: string | null = null;
	author: string | null = null;

	/**
	 * The constructor for the SurMlFile class.
	 *
	 * model: the model to be saved
	 * name: the name of the model
	 * engine: which Engine to use for conversion
	 */
	constructor(
		model: any = null, 
		name: string | null = null, 
		engine: Engine | null = null
	) {
		this.model = model;
		this.name = name;
		this.engine = engine;
		this.fileId = this.cacheModel();
		this.rustAdapter = new RustAdapter(this.fileId!, this.engine!);
	}

	/**
	 * Caches a model so it can be loaded as raw bytes to be fused with the header.
	 *
	 * returns: the file id of the model so it can be retrieved from the cache
	 */
	private cacheModel(): string | null {
		if (
			this.model == null ||
			this.name == null ||
			this.engine == null
		) {
			return null;
		};

		let rawFilePath: string;
		if (this.engine === Engine.ONNX) {
			rawFilePath = OnnxAdapter.saveModelToOnnx(this.model);
		} else {
			throw new Error(`Engine ${this.engine} not supported`);
		};

		return RustAdapter.passRawModelIntoRust(rawFilePath);
	}

	/**
	 * Adds a column to the model metadata (order matters).
	 *
	 * name: the name of the column
	 */
	addColumn(name: string): void {
		this.rustAdapter.addColumn(name);
	}

	/**
	 * Adds an output to the model metadata.
	 *
	 * outputName: the name of the output
	 * normaliserType: the type of normaliser to use
	 * one: the first parameter of the normaliser
	 * two: the second parameter of the normaliser
	 */
	addOutput(outputName: string, normaliserType: string, one: number, two: number): void {
		this.rustAdapter.addOutput(outputName, normaliserType, one, two);
	}

	/**
	 * Adds a description to the model metadata.
	 *
	 * description: the description of the model
	 */
	addDescription(description: string): void {
		this.description = description;
		this.rustAdapter.addDescription(description);
	}

	/**
	 * Adds a version to the model metadata.
	 *
	 * version: the version of the model
	 */
	addVersion(version: string): void {
		this.version = version;
		this.rustAdapter.addVersion(version);
	}

	/**
	 * Adds a name to the model metadata.
	 *
	 * name: the name of the model
	 */
	addName(name: string): void {
		this.name = name;
		this.rustAdapter.addName(name);
	}

	/**
	 * Adds a normaliser to the model metadata for a column.
	 *
	 * columnName: the name of the column
	 * normaliserType: the type of normaliser to use
	 * one: the first parameter of the normaliser
	 * two: the second parameter of the normaliser
	 */
	addNormaliser(
		columnName: string,
		normaliserType: string,
		one: number,
		two: number
	): void {
		this.rustAdapter.addNormaliser(columnName, normaliserType, one, two);
	}

	/**
	 * Adds an author to the model metadata.
	 *
	 * author: the author of the model
	 */
	addAuthor(author: string): void {
		this.author = author;
		this.rustAdapter.addAuthor(author);
	}

	/**
	 * Saves the model to a file.
	 *
	 * path: the path to save the model to
	 */
	save(path: string): void {
		this.rustAdapter.save(path, this.name);
	}

	/**
	 * Converts the model to bytes.
	 *
	 * returns: the model as bytes
	 */
	toBytes(): Uint8Array {
		return this.rustAdapter.toBytes();
	}

	/**
	 * Loads a model from a file so compute operations can be done.
	 *
	 * path: the path to load the model from
	 * engine: the engine to use to load the model
	 *
	 * returns: a new SurMlFile with loaded model and engine definition
	 */
	static load(path: string, engine: Engine): SurMlFile {
		const instance = new SurMlFile();
		const [fileId, name, description, version] = RustAdapter.load(path);
		instance.fileId = fileId;
		instance.name = name;
		instance.description = description;
		instance.version = version;
		instance.engine = engine;
		instance.rustAdapter = new RustAdapter(fileId, engine);
		return instance;
	}

	/**
	 * Uploads a model to a remote server.
	 *
	 * path: the path to load the model from
	 * url: the url of the remote server
	 * chunkSize: the size of each chunk to upload
	 * namespace: the namespace of the remote server
	 * database: the database of the remote server
	 * username: the username of the remote server (optional)
	 * password: the password of the remote server (optional)
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
		RustAdapter.upload(path, url, chunkSize, namespace, database, username, password);
	}

	/**
	 * Calculates an output from the model given an input vector.
	 *
	 * inputVector: a 1D vector of inputs to the model
	 *
	 * returns: the output of the model
	 */
	rawCompute(inputVector: number[]): number[] {
		return this.rustAdapter.rawCompute(inputVector);
	}

	/**
	 * Calculates an output from the model given a value map.
	 *
	 * valueMap: a dictionary of inputs to the model with the column names as keys and floats as values
	 *
	 * returns: the output of the model
	 */
	bufferedCompute(valueMap: Record<string, number>): number[] {
		return this.rustAdapter.bufferedCompute(valueMap);
	}
}
