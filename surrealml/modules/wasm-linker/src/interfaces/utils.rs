//! Utils for returning C types and processing null pointers for raw C inputs.
use crate::interfaces::tokenizers::TokenizerHandle;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};

/// Checks that the pointer to the string is not null and converts to a Rust `String`.
/// On error, returns early from the enclosing function with a `TokenizerReturn::error`.
///
/// # Arguments
/// * `str_ptr` — The pointer to the C string.
/// * `var_name` — The name of the variable (for error messages).
#[macro_export]
macro_rules! process_string_for_tokenizer_return {
    ($str_ptr:expr, $var_name:expr) => {{
        // Capture the metavariable so it isn’t expanded inside `unsafe`.
        let ptr = $str_ptr;

        if ptr.is_null() {
            return TokenizerReturn::error(format!("Received null pointer for {}", $var_name));
        }

        // SAFETY: `ptr` is non-null and, by the FFI contract, points to a valid
        // NUL-terminated C string.
        let c_str = unsafe { ::std::ffi::CStr::from_ptr(ptr) };

        match c_str.to_str() {
            Ok(s) => s.to_owned(),
            Err(_) => {
                return TokenizerReturn::error(format!("Invalid UTF-8 for {}", $var_name));
            }
        }
    }};
}

/// Like `process_string_for_tokenizer_return!`, but yields an `Option<String>`.
/// If `str_ptr` is null, produces `None`. If invalid UTF-8, returns an error.
///
/// # Arguments
/// * `str_ptr` — The pointer to the C string.
/// * `var_name` — The name of the variable (for error messages).
#[macro_export]
macro_rules! process_opt_string_for_tokenizer_return {
    ($str_ptr:expr, $var_name:expr) => {{
        // Bind the argument first so the metavariable is outside the `unsafe` block.
        let ptr = $str_ptr;

        // Early-out if it’s null.
        if ptr.is_null() {
            None
        } else {
            // SAFETY: `ptr` is guaranteed non-null and points to a valid, NUL-terminated C string
            // supplied by the caller’s FFI contract.
            let c_str = unsafe { ::std::ffi::CStr::from_ptr(ptr) };

            match c_str.to_str() {
                Ok(s) => Some(s.to_owned()),
                Err(_) => {
                    return TokenizerReturn::error(format!("Invalid UTF-8 for {}", $var_name));
                }
            }
        }
    }};
}

/// Returned from `load_tokenizer_ffi`, wrapping either a valid handle or an error.
#[repr(C)]
pub struct TokenizerReturn {
    /// On success, a non-null pointer to a `TokenizerHandle`; on error, null.
    pub handle: *mut TokenizerHandle,
    pub is_error: c_int,
    pub error_message: *mut c_char,
}

impl TokenizerReturn {
    /// Builds a success return value containing `handle`.
    ///
    /// # Arguments
    /// * `handle` — The opaque tokenizer handle to return.
    ///
    /// # Returns
    /// A `TokenizerReturn` with `is_error = 0` and `error_message = NULL`.
    pub fn success(handle: *mut TokenizerHandle) -> Self {
        TokenizerReturn {
            handle,
            is_error: 0,
            error_message: std::ptr::null_mut(),
        }
    }

    /// Builds an error return value with the given message.
    ///
    /// # Arguments
    /// * `msg` — The error message to pass back as a C string.
    ///
    /// # Returns
    /// A `TokenizerReturn` with `is_error = 1`, `handle = NULL`, and `error_message` set.
    pub fn error(msg: String) -> Self {
        let c_msg = CString::new(msg).unwrap().into_raw();
        TokenizerReturn {
            handle: std::ptr::null_mut(),
            is_error: 1,
            error_message: c_msg,
        }
    }
}

/// Frees solely the error message memory associated with a `TokenizerReturn`,
///
/// # Arguments
/// * `tokenizer_return` — The `TokenizerReturn` to clean up.
#[unsafe(no_mangle)]
pub extern "C" fn free_tokenizer_return(tokenizer_return: TokenizerReturn) {
    // Free the error message if present
    if !tokenizer_return.error_message.is_null() {
        unsafe { drop(CString::from_raw(tokenizer_return.error_message)) }
    }

    // Free the tokenizer handle if present
    if !tokenizer_return.handle.is_null() {
        unsafe { drop(Box::from_raw(tokenizer_return.handle)) }
    }
}

/// Checks that the pointer to the string is not null and converts to a Rust string. Any errors are returned as a `StringReturn`.
///
/// # Arguments
/// * `str_ptr` - The pointer to the string.
/// * `var_name` - The name of the variable being processed (for error messages).
#[macro_export]
macro_rules! process_string_for_string_return {
    ($str_ptr:expr, $var_name:expr) => {
        let ptr = $str_ptr;
        match $str_ptr.is_null() {
            true => {
                return StringReturn {
                    is_error: 1,
                    error_message: CString::new(format!(
                        "Received a null pointer for {}",
                        $var_name
                    ))
                    .unwrap()
                    .into_raw(),
                    string: std::ptr::null_mut(),
                };
            }
            false => {
                let c_str = unsafe { CStr::from_ptr(ptr) };
                match c_str.to_str() {
                    Ok(s) => s.to_owned(),
                    Err(_) => {
                        return StringReturn {
                            is_error: 1,
                            error_message: CString::new(format!(
                                "Invalid UTF-8 string received for {}",
                                $var_name
                            ))
                            .unwrap()
                            .into_raw(),
                            string: std::ptr::null_mut(),
                        };
                    }
                }
            }
        }
    };
}

/// Returns a simple String to the caller.
///
/// # Fields
/// * `string` - The string to return.
/// * `is_error` - A flag indicating if an error occurred (1 if error 0 if not).
/// * `error_message` - An optional error message.
#[repr(C)]
pub struct StringReturn {
    pub string: *mut c_char,
    pub is_error: c_int,
    pub error_message: *mut c_char,
}

impl StringReturn {
    /// Returns a new `StringReturn` object with the string and no error.
    ///
    /// # Notes
    /// This is allowed as dead code so we can use this outside of the program
    ///
    /// # Arguments
    /// * `string` - The string to return.
    ///
    /// # Returns
    /// A new `StringReturn` object.
    pub fn success(string: String) -> Self {
        StringReturn {
            string: CString::new(string).unwrap().into_raw(),
            is_error: 0,
            error_message: std::ptr::null_mut(),
        }
    }
}

/// Frees the memory allocated for the `StringReturn` object.
///
/// # Arguments
/// * `string_return` - The `StringReturn` object to free.
#[unsafe(no_mangle)]
pub extern "C" fn free_string_return(string_return: StringReturn) {
    // Free the string if it is not null
    if !string_return.string.is_null() {
        unsafe { drop(CString::from_raw(string_return.string)) };
    }
    // Free the error message if it is not null
    if !string_return.error_message.is_null() {
        unsafe { drop(CString::from_raw(string_return.error_message)) };
    }
}

/// Checks that the pointer to the string is not null and converts it to a Rust `&str`.  
/// On error, returns early from the enclosing function with a `VecU32Return::error`.  
///  
/// # Arguments  
/// * `str_ptr` — The pointer to the C string.  
/// * `var_name` — The name of the variable being processed (for error messages).  
#[macro_export]
macro_rules! process_string_for_vecu32_return {
    ($str_ptr:expr, $var_name:expr) => {{
        // Bind macro arguments first; nothing unsafe here, so Clippy is happy.
        let ptr = $str_ptr;

        if ptr.is_null() {
            return VecU32Return::error(format!("Received null pointer for {}", $var_name));
        }

        // SAFETY: `ptr` is non-null and (by the FFI contract of the caller) points to a
        // valid, NUL-terminated C string.
        let c_str = unsafe { ::std::ffi::CStr::from_ptr(ptr) };

        match c_str.to_str() {
            Ok(s) => s,
            Err(_) => return VecU32Return::error(format!("Invalid UTF-8 for {}", $var_name)),
        }
    }};
}

/// Returns a vector of bytes to the caller.
///
/// # Fields
/// * `data` - The pointer to the data.
/// * `length` - The length of the data.
/// * `capacity` - The capacity of the data.
/// * `is_error` - A flag indicating if an error occurred (1 if error 0 if not).
/// * `error_message` - An optional error message.
#[repr(C)]
pub struct VecU32Return {
    pub data: *mut u32,
    pub length: usize,
    pub capacity: usize, // Optional if you want to include capacity for clarity
    pub is_error: c_int,
    pub error_message: *mut c_char,
}

impl VecU32Return {
    /// Returns a new `VecU32Return` object with the data and no error.
    ///
    /// # Arguments
    /// * `data` - The data to return.
    ///
    /// # Returns
    /// A new `VecU32Return` object.
    pub fn success(data: Vec<u32>) -> Self {
        let mut data = data;
        let data_ptr = data.as_mut_ptr();
        let length = data.len();
        let capacity = data.capacity();
        std::mem::forget(data);
        VecU32Return {
            data: data_ptr,
            length,
            capacity,
            is_error: 0,
            error_message: std::ptr::null_mut(),
        }
    }

    pub fn error(msg: String) -> Self {
        let c_msg = CString::new(msg).unwrap().into_raw();
        VecU32Return {
            data: std::ptr::null_mut(),
            length: 0,
            capacity: 0,
            is_error: 1,
            error_message: c_msg,
        }
    }
}

/// Frees only the string memory allocated for the `VecU32Return` object.
///
/// # Arguments
/// * `vec_u32` - The `VecU32Return` object to free.
#[unsafe(no_mangle)]
pub extern "C" fn free_vec_u32_return(vec_u32_return: VecU32Return) {
    if !vec_u32_return.error_message.is_null() {
        unsafe { drop(CString::from_raw(vec_u32_return.error_message)) };
    }

    if !vec_u32_return.data.is_null() {
        unsafe {
            drop(Vec::from_raw_parts(
                vec_u32_return.data,
                vec_u32_return.length,
                vec_u32_return.capacity,
            ))
        };
    }
}
