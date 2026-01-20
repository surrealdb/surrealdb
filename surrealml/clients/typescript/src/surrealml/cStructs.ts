/**
 * Defines all the C structs that are returned from the C lib.
 */
import koffi from 'koffi';


/** 
 * Define the disposables so that we auto drop the heap memory for these fields
 * when converting to JS.
 */
const HeapStr = koffi.disposable('HeapStr', 'char *');
const FloatPtr = koffi.disposable("FloatPtr", "float *");
const BytePtr  = koffi.disposable("BytePtr" , "int8_t *");  

/**
 * A return type that just returns a string
 *
 * Fields:
 *     string: the string that is being returned (only present if successful)
 *     is_error: 1 if error, 0 if not
 *     error_message: the error message (only present if error)
 */
const StringReturn = koffi.struct('StringReturn', {
	string: HeapStr,
	is_error: 'int',
	error_message: HeapStr
});

/**
 * A return type that just returns nothing
 *
 * Fields:
 *     is_error: 1 if error, 0 if not
 *     error_message: the error message (only present if error)
 */
const EmptyReturn = koffi.struct('EmptyReturn', {
	is_error: 'int',
	error_message: HeapStr
});

/**
 * A return type when loading the meta of a surml file.
 *
 * Fields:
 *     file_id: a unique identifier for the file in the state of the C lib
 *     name: a name of the model
 *     description: a description of the model
 *     version: the version of the model
 *     error_message: the error message (only present if error)
 *     is_error: 1 if error, 0 if not
 */
const FileInfo = koffi.struct('FileInfo', {
	file_id: HeapStr,
	name: HeapStr,
	description: HeapStr,
	version: HeapStr,
	error_message: HeapStr,
	is_error: 'int'
});

/**
 * A return type when retrieving a vector of f32 from the C lib.
 *
 * Fields:
 *     data: pointer to float array
 *     length: the length of the array
 *     capacity: the capacity of the array
 *     is_error: 1 if error, 0 if not
 *     error_message: the error message (only present if error)
 */
const Vecf32Return = koffi.struct('Vecf32Return', {
	data: FloatPtr,
	length: 'size_t',
	capacity: 'size_t',
	is_error: 'int',
	error_message: HeapStr
});

/**
 * A return type returning bytes.
 *
 * Fields:
 *     data: pointer to int8 array (bytes)
 *     length: the length of the vector
 *     capacity: the capacity of the vector
 *     is_error: 1 if error, 0 if not
 *     error_message: the error message (only present if error)
 */
const VecU8Return = koffi.struct('VecU8Return', {
	data: BytePtr,
	length: 'size_t',
	capacity: 'size_t',
	is_error: 'int',
	error_message: HeapStr
});

export {
	StringReturn,
	EmptyReturn,
	FileInfo,
	Vecf32Return,
	VecU8Return
};

/**
 * Manually defined interfaces matching the struct instances.
 */
export interface StringReturnType {
	string: string;
	is_error: number;
	error_message: string | null;
  }
  
  export interface EmptyReturnType {
	is_error: number;
	error_message: string | null;
  }
  
  export interface FileInfoType {
	file_id: string;
	name: string;
	description: string;
	version: string;
	error_message: string | null;
	is_error: number;
  }
  
  export interface Vecf32ReturnType {
	data: Float32Array;
	length: number;
	capacity: number;
	is_error: number;
	error_message: string | null;
  }
  
  export interface VecU8ReturnType {
	data: Uint8Array;
	length: number;
	capacity: number;
	is_error: number;
	error_message: string | null;
  }
  