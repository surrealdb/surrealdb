//! This module contains the buffered_compute function that is called from the C API to compute the model.
use crate::state::STATE;
use crate::utils::Vecf32Return;
use std::collections::HashMap;
use std::ffi::{c_char, c_float, c_int, CStr, CString};
use surrealml_core::execution::compute::ModelComputation;

/// Computes the model with the given data.
///
/// # Arguments
/// * `file_id_ptr` - A pointer to the unique identifier for the loaded model.
/// * `data_ptr` - A pointer to the data to compute.
/// * `length` - The length of the data.
/// * `strings` - A pointer to an array of strings to use as keys for the data.
/// * `string_count` - The number of strings in the array.
///
/// # Returns
/// A Vecf32Return object containing the outcome of the computation.
#[no_mangle]
pub extern "C" fn buffered_compute(
    file_id_ptr: *const c_char,
    data_ptr: *const c_float,
    data_length: usize,
    strings: *const *const c_char,
    string_count: c_int,
) -> Vecf32Return {
    if file_id_ptr.is_null() {
        return Vecf32Return {
            data: std::ptr::null_mut(),
            length: 0,
            capacity: 0,
            is_error: 1,
            error_message: CString::new("File id is null").unwrap().into_raw(),
        };
    }
    if data_ptr.is_null() {
        return Vecf32Return {
            data: std::ptr::null_mut(),
            length: 0,
            capacity: 0,
            is_error: 1,
            error_message: CString::new("Data is null").unwrap().into_raw(),
        };
    }

    let file_id = match unsafe { CStr::from_ptr(file_id_ptr) }.to_str() {
        Ok(file_id) => file_id.to_owned(),
        Err(error) => {
            return Vecf32Return {
                data: std::ptr::null_mut(),
                length: 0,
                capacity: 0,
                is_error: 1,
                error_message: CString::new(format!("Error getting file id: {}", error))
                    .unwrap()
                    .into_raw(),
            }
        }
    };

    if strings.is_null() {
        return Vecf32Return {
            data: std::ptr::null_mut(),
            length: 0,
            capacity: 0,
            is_error: 1,
            error_message: CString::new("string pointer is null").unwrap().into_raw(),
        };
    }

    // extract the list of strings from the C array
    let string_count = string_count as usize;
    let c_strings = unsafe { std::slice::from_raw_parts(strings, string_count) };
    let rust_strings: Vec<String> = c_strings
        .iter()
        .map(|&s| {
            if s.is_null() {
                String::new()
            } else {
                unsafe { CStr::from_ptr(s).to_string_lossy().into_owned() }
            }
        })
        .collect();
    for i in rust_strings.iter() {
        if i.is_empty() {
            return Vecf32Return {
                data: std::ptr::null_mut(),
                length: 0,
                capacity: 0,
                is_error: 1,
                error_message: CString::new("null string passed in as key")
                    .unwrap()
                    .into_raw(),
            };
        }
    }

    let data_slice = unsafe { std::slice::from_raw_parts(data_ptr, data_length) };

    if rust_strings.len() != data_slice.len() {
        return Vecf32Return {
            data: std::ptr::null_mut(),
            length: 0,
            capacity: 0,
            is_error: 1,
            error_message: CString::new("String count does not match data length")
                .unwrap()
                .into_raw(),
        };
    }

    // stitch the strings and data together
    let mut input_map = HashMap::new();
    for (i, key) in rust_strings.iter().enumerate() {
        input_map.insert(key.clone(), data_slice[i]);
    }

    let mut state = match STATE.lock() {
        Ok(state) => state,
        Err(error) => {
            return Vecf32Return {
                data: std::ptr::null_mut(),
                length: 0,
                capacity: 0,
                is_error: 1,
                error_message: CString::new(format!("Error getting state: {}", error))
                    .unwrap()
                    .into_raw(),
            }
        }
    };
    let file = match state.get_mut(&file_id) {
        Some(file) => file,
        None => {
            return Vecf32Return {
                data: std::ptr::null_mut(),
                length: 0,
                capacity: 0,
                is_error: 1,
                error_message: CString::new(format!(
                    "File not found for id: {}, here is the state: {:?}",
                    file_id,
                    state.keys()
                ))
                .unwrap()
                .into_raw(),
            }
        }
    };
    let compute_unit = ModelComputation { surml_file: file };
    match compute_unit.buffered_compute(&mut input_map) {
        Ok(mut output) => {
            let output_len = output.len();
            let output_capacity = output.capacity();
            let output_ptr = output.as_mut_ptr();
            std::mem::forget(output);
            Vecf32Return {
                data: output_ptr,
                length: output_len,
                capacity: output_capacity,
                is_error: 0,
                error_message: std::ptr::null_mut(),
            }
        }
        Err(error) => Vecf32Return {
            data: std::ptr::null_mut(),
            length: 0,
            capacity: 0,
            is_error: 1,
            error_message: CString::new(format!("Error computing model: {}", error))
                .unwrap()
                .into_raw(),
        },
    }
}
