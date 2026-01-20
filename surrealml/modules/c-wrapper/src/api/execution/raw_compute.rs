//! This module contains the raw_compute function that is called from the C API to compute the model.
use crate::state::STATE;
use crate::utils::Vecf32Return;
use std::ffi::{c_char, c_float, CStr, CString};
use surrealml_core::execution::compute::ModelComputation;

/// Computes the model with the given data.
///
/// # Arguments
/// * `file_id_ptr` - A pointer to the unique identifier for the loaded model.
/// * `data_ptr` - A pointer to the data to compute.
/// * `length` - The length of the data.
///
/// # Returns
/// A Vecf32Return object containing the outcome of the computation.
#[no_mangle]
pub extern "C" fn raw_compute(
    file_id_ptr: *const c_char,
    data_ptr: *const c_float,
    length: usize,
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

    let slice = unsafe { std::slice::from_raw_parts(data_ptr, length) };
    let tensor = ndarray::arr1(slice).into_dyn();
    let compute_unit = ModelComputation { surml_file: file };

    // perform the computation
    let mut outcome = match compute_unit.raw_compute(tensor, None) {
        Ok(outcome) => outcome,
        Err(error) => {
            return Vecf32Return {
                data: std::ptr::null_mut(),
                length: 0,
                capacity: 0,
                is_error: 1,
                error_message: CString::new(format!("Error computing model: {}", error.message))
                    .unwrap()
                    .into_raw(),
            }
        }
    };
    let outcome_ptr = outcome.as_mut_ptr();
    let outcome_len = outcome.len();
    let outcome_capacity = outcome.capacity();
    std::mem::forget(outcome);
    Vecf32Return {
        data: outcome_ptr,
        length: outcome_len,
        capacity: outcome_capacity,
        is_error: 0,
        error_message: std::ptr::null_mut(),
    }
}
