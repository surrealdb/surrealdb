// src/types/koffi.d.ts

/**
 * Augment the default `koffi` module to expose `view(ptr, len)`.
 */
declare module 'koffi' {
	// Describe the shape of the default export
	interface Koffi {
	/**
	 * Create a zero‐copy ArrayBuffer view of native memory.
	 */
	view(ptr: any, length: number): ArrayBuffer;
	// allow any other koffi methods to still be callable
	[key: string]: any;
	}

	// Tell TS that `import koffi from 'koffi'` yields this interface:
	const koffi: Koffi;
	export default koffi;
}
  