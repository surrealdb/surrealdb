import { OnnxAdapter } from './onnx.js';


/**
 * Specifies which execution engine to use for a given model.
 *
 * - PYTORCH:    run via PyTorch and export through ONNX.
 * - NATIVE:     run natively in Rust (using Linfa).
 * - SKLEARN:    run via scikit-learn and export through ONNX.
 * - TENSORFLOW: run via TensorFlow and export through ONNX.
 * - ONNX:       run directly on an existing ONNX model (no conversion).
 */
export enum Engine {
	NATIVE = 'native',
	ONNX = 'onnx',
}
