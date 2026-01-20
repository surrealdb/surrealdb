//! Defines macros and C structs for reducing the amount of boilerplate code required for the C API.
use std::ffi::CString;
use std::os::raw::{c_char, c_int};

/// Checks that the pointer to the string is not null and converts to a Rust string. Any errors are returned as an `EmptyReturn`.
///
/// # Arguments
/// * `str_ptr` - The pointer to the string.
/// * `var_name` - The name of the variable being processed (for error messages).
#[macro_export]
macro_rules! process_string_for_empty_return {
    ($str_ptr:expr, $var_name:expr) => {
        match $str_ptr.is_null() {
            true => {
                return EmptyReturn {
                    is_error: 1,
                    error_message: CString::new(format!(
                        "Received a null pointer for {}",
                        $var_name
                    ))
                    .unwrap()
                    .into_raw(),
                };
            }
            false => {
                let str_ptr = $str_ptr;
                let c_str = unsafe { CStr::from_ptr(str_ptr) };
                match c_str.to_str() {
                    Ok(s) => s.to_owned(),
                    Err(_) => {
                        return EmptyReturn {
                            is_error: 1,
                            error_message: CString::new(format!(
                                "Invalid UTF-8 string received for {}",
                                $var_name
                            ))
                            .unwrap()
                            .into_raw(),
                        };
                    }
                }
            }
        }
    };
    ($str_ptr:expr, $var_name:expr, Option) => {
        match $str_ptr.is_null() {
            true => {
                return None;
            }
            false => {
                let c_str = unsafe { CStr::from_ptr($str_ptr) };
                match c_str.to_str() {
                    Ok(s) => Some(s.to_owned()),
                    Err(_) => {
                        return EmptyReturn {
                            is_error: 1,
                            error_message: CString::new(format!(
                                "Invalid UTF-8 string received for {}",
                                $var_name
                            ))
                            .unwrap()
                            .into_raw(),
                        };
                    }
                }
            }
        }
    };
}

/// Checks that the pointer to the string is not null and converts to a Rust string. Any errors are returned as a `StringReturn`.
///
/// # Arguments
/// * `str_ptr` - The pointer to the string.
/// * `var_name` - The name of the variable being processed (for error messages).
#[macro_export]
macro_rules! process_string_for_string_return {
    ($str_ptr:expr, $var_name:expr) => {
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
                let str_ptr = $str_ptr;
                let c_str = unsafe { CStr::from_ptr(str_ptr) };
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

/// Checks that the pointer to the string is not null and converts to a Rust string. Any errors are returned as a `VecU8Return`.
///
/// # Arguments
/// * `str_ptr` - The pointer to the string.
/// * `var_name` - The name of the variable being processed (for error messages).
#[macro_export]
macro_rules! process_string_for_vec_u8_return {
    ($str_ptr:expr, $var_name:expr) => {
        match $str_ptr.is_null() {
            true => {
                return VecU8Return {
                    data: std::ptr::null_mut(),
                    length: 0,
                    capacity: 0,
                    is_error: 1,
                    error_message: CString::new(format!(
                        "Received a null pointer for {}",
                        $var_name
                    ))
                    .unwrap()
                    .into_raw(),
                };
            }
            false => {
                let str_ptr = $str_ptr;
                let c_str = unsafe { CStr::from_ptr(str_ptr) };
                match c_str.to_str() {
                    Ok(s) => s.to_owned(),
                    Err(_) => {
                        return VecU8Return {
                            data: std::ptr::null_mut(),
                            length: 0,
                            capacity: 0,
                            is_error: 1,
                            error_message: CString::new(format!(
                                "Invalid UTF-8 string received for {}",
                                $var_name
                            ))
                            .unwrap()
                            .into_raw(),
                        };
                    }
                }
            }
        }
    };
}

/// Checks the result of an execution and returns an `StringReturn` if an error occurred.
///
/// # Arguments
/// * `execution` - The execution such as a function call to map to `StringReturn` if an error occurred.
#[macro_export]
macro_rules! string_return_safe_eject {
    ($execution:expr) => {
        match $execution {
            Ok(s) => s,
            Err(e) => {
                return StringReturn {
                    string: std::ptr::null_mut(),
                    is_error: 1,
                    error_message: CString::new(e.to_string()).unwrap().into_raw(),
                }
            }
        }
    };
}

/// Checks the result of an execution and returns an `EmptyReturn` if an error occurred or a none is returned.
///
/// # Arguments
/// * `execution` - The execution such as a function call to map to `EmptyReturn` if an error occurred.
/// * `var` - The variable name to include in the error message.
/// * `Option` - The type of the execution.
///
/// # Arguments
/// * `execution` - The execution such as a function call to map to `EmptyReturn` if an error occurred.
#[macro_export]
macro_rules! empty_return_safe_eject {
    ($execution:expr, $var:expr, Option) => {
        match $execution {
            Some(s) => s,
            None => {
                return EmptyReturn {
                    is_error: 1,
                    error_message: CString::new($var).unwrap().into_raw(),
                }
            }
        }
    };
    ($execution:expr) => {
        match $execution {
            Ok(s) => s,
            Err(e) => {
                return EmptyReturn {
                    is_error: 1,
                    error_message: CString::new(e.to_string()).unwrap().into_raw(),
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
#[no_mangle]
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

/// Returns a simple empty return object to the caller.
///
/// # Fields
/// * `is_error` - A flag indicating if an error occurred (1 if error 0 if not).
/// * `error_message` - An optional error message.
#[repr(C)]
pub struct EmptyReturn {
    pub is_error: c_int,            // 0 for success, 1 for error
    pub error_message: *mut c_char, // Optional error message
}

impl EmptyReturn {
    /// Returns a new `EmptyReturn` object with no error.
    ///
    /// # Returns
    /// A new `EmptyReturn` object.
    pub fn success() -> Self {
        EmptyReturn {
            is_error: 0,
            error_message: std::ptr::null_mut(),
        }
    }
}

/// Frees the memory allocated for the `EmptyReturn` object.
///
/// # Arguments
/// * `empty_return` - The `EmptyReturn` object to free.
#[no_mangle]
pub extern "C" fn free_empty_return(empty_return: EmptyReturn) {
    // Free the error message if it is not null
    if !empty_return.error_message.is_null() {
        unsafe { drop(CString::from_raw(empty_return.error_message)) };
    }
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
pub struct VecU8Return {
    pub data: *mut u8,
    pub length: usize,
    pub capacity: usize, // Optional if you want to include capacity for clarity
    pub is_error: c_int,
    pub error_message: *mut c_char,
}

impl VecU8Return {
    /// Returns a new `VecU8Return` object with the data and no error.
    ///
    /// # Arguments
    /// * `data` - The data to return.
    ///
    /// # Returns
    /// A new `VecU8Return` object.
    pub fn success(data: Vec<u8>) -> Self {
        let mut data = data;
        let data_ptr = data.as_mut_ptr();
        let length = data.len();
        let capacity = data.capacity();
        std::mem::forget(data);
        VecU8Return {
            data: data_ptr,
            length,
            capacity,
            is_error: 0,
            error_message: std::ptr::null_mut(),
        }
    }
}

/// Frees the memory allocated for the `VecU8Return` object.
///
/// # Arguments
/// * `vec_u8` - The `VecU8Return` object to free.
#[no_mangle]
pub extern "C" fn free_vec_u8(vec_u8: VecU8Return) {
    // Free the data if it is not null
    if !vec_u8.data.is_null() {
        unsafe {
            drop(Vec::from_raw_parts(
                vec_u8.data,
                vec_u8.length,
                vec_u8.capacity,
            ))
        };
    }
}

/// Holds the data around the outcome of the raw_compute function.
///
/// # Fields
/// * `data` - The data returned from the computation.
/// * `length` - The length of the data.
/// * `capacity` - The capacity of the data.
/// * `is_error` - A flag indicating if an error occurred (1 for error, 0 for success).
/// * `error_message` - An error message if the computation failed.
#[repr(C)]
pub struct Vecf32Return {
    pub data: *mut f32,
    pub length: usize,
    pub capacity: usize, // Optional if you want to include capacity for clarity
    pub is_error: c_int,
    pub error_message: *mut c_char,
}

/// Frees the memory allocated for the Vecf32Return.
///
/// # Arguments
/// * `vecf32_return` - The Vecf32Return to free.
#[no_mangle]
pub extern "C" fn free_vecf32_return(vecf32_return: Vecf32Return) {
    // Free the data if it is not null
    if !vecf32_return.data.is_null() {
        unsafe {
            drop(Vec::from_raw_parts(
                vecf32_return.data,
                vecf32_return.length,
                vecf32_return.capacity,
            ))
        };
    }
    // Free the error message if it is not null
    if !vecf32_return.error_message.is_null() {
        unsafe { drop(CString::from_raw(vecf32_return.error_message)) };
    }
}
