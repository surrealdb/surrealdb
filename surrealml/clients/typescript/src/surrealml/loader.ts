/**
 * The loader for the dynamic C lib written in Rust.
 */
import koffi from 'koffi';
import fs from 'fs';
import os from 'os';
import path from 'path';

import { EmptyReturn, StringReturn, Vecf32Return, FileInfo, VecU8Return } from './cStructs.js';
import { readDynamicLibVersion } from './utils.js';


const DYNAMIC_LIB_VERSION = readDynamicLibVersion();
const CACHE_ROOT_DIR = path.join(os.homedir(), 'surrealml_deps');
const CORE_LIB_DIR = path.join(CACHE_ROOT_DIR, 'core_ml_lib', DYNAMIC_LIB_VERSION);

const SUFFIX: Record<string, string> = { linux: '.so', darwin: '.dylib', win32: '.dll' };

/**
 * Load the shared library from the local cache.
 * Default base name of the library without extension is "libc_wrapper".
 * Returns the loaded Koffi library instance.
 * Throws an error if the OS is unsupported or the library file is not found.
 */
export function loadLibrary(base = 'libc_wrapper') {
	const suff = SUFFIX[process.platform];
	if (!suff) throw new Error(`Unsupported OS: ${process.platform}`);

	const libPath = path.resolve(CORE_LIB_DIR, `${base}${suff}`);
	if (!fs.existsSync(libPath)) {
		throw new Error(`Shared library not found at ${libPath}`);
	}

	const lib = koffi.load(libPath);
	if (!lib) {
		throw new Error(
			`koffi.load returned undefined for ${libPath}`
		);
	}
	return lib;
}

/**
 * Get the ONNX runtime library filename for the current OS.
 * Returns the filename of the ONNX runtime shared library.
 * Throws an error if the OS is unsupported.
 */
export function getOnnxLibName(): string {
	switch (process.platform) {
	case 'win32':
		return 'libonnxruntime.dll';
	case 'darwin':
		return 'libonnxruntime.dylib';
	case 'linux':
		return 'libonnxruntime.so';
	default:
		throw new Error(`Unsupported operating system: ${process.platform}`);
	}
}

/**
 * Singleton loader for the dynamic C library.
 */
export class LibLoader {
	private static instance?: LibLoader;
	public  readonly lib: any;            

	private constructor(libName = 'libc_wrapper') {
		this.lib = loadLibrary(libName);   

		[
			'add_name', 'add_description', 'add_version',
			'add_column', 'add_author', 'add_origin', 'add_engine'
		].forEach(fn =>
			this.lib[fn] = this.lib.func(fn, EmptyReturn, ['char *', 'char *'])
		);

		this.lib.add_output = this.lib.func(
			'add_output',
			EmptyReturn,
			['char *', 'char *', 'char *', 'char *', 'char *']
		);
		  
		this.lib.add_normaliser = this.lib.func(
			'add_normaliser',
			EmptyReturn,
			['char *', 'char *', 'char *', 'char *', 'char *']
		);

		this.lib.load_model = this.lib.func('load_model', FileInfo, ['char *']);
		this.lib.load_cached_raw_model = this.lib.func('load_cached_raw_model', StringReturn, ['char *']);
		this.lib.to_bytes = this.lib.func('to_bytes', VecU8Return, ['char *']);
		this.lib.save_model = this.lib.func('save_model', EmptyReturn, ['char *', 'char *']);
		this.lib.upload_model = this.lib.func(
			'upload_model', EmptyReturn,
			['char *', 'char *', 'size_t', 'char *', 'char *', 'char *', 'char *']
		);

		this.lib.raw_compute = this.lib.func(
			'raw_compute', Vecf32Return,
			['char *', 'float *', 'size_t']
		);

		this.lib.buffered_compute = this.lib.func(
			'buffered_compute', Vecf32Return,
			['char *', 'float *', 'size_t', 'char **', 'int']
		);

		this.lib.link_onnx = this.lib.func('link_onnx', EmptyReturn, []);
		const info = this.lib.link_onnx();
		if (info.is_error === 1) {
			// this.lib.free_empty_return(info);
			throw new Error(`Failed to load onnxruntime: ${info.error_message.toString('utf8')}`);
		}
		// this.lib.free_empty_return(info);
	}

	/** Retrieve singleton instance, creating it on first call. */
	static getInstance(): LibLoader {
		if (!LibLoader.instance) LibLoader.instance = new LibLoader();
		return LibLoader.instance;
	}
}

