//! The kernels a program may call: what a capability manifest for it names.
//!
//! [`kernels`] walks a program and every unit it holds, and names the kernels
//! (`idx::kernels`) each source, instruction and index-function call reaches.
