//! Save a model to a file, deleting the file from the `STATE` in the process.
// Standard library imports
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

// External crate imports
use surrealml_core::storage::surml_file::SurMlFile;

// Local module imports
use crate::state::STATE;
use crate::utils::EmptyReturn;
use crate::{empty_return_safe_eject, process_string_for_empty_return};

/// Saves a model to a file, deleting the file from the `PYTHON_STATE` in the process.
///
/// # Arguments
/// * `file_path` - The path to the file to save to.
/// * `file_id` - The unique identifier for the loaded model.
///
/// # Returns
/// An empty return object indicating success or failure.
#[no_mangle]
pub extern "C" fn save_model(
    file_path_ptr: *const c_char,
    file_id_ptr: *const c_char,
) -> EmptyReturn {
    let file_path_str = process_string_for_empty_return!(file_path_ptr, "file path");
    let file_id_str = process_string_for_empty_return!(file_id_ptr, "file id");
    let mut state = STATE.lock().unwrap();
    let file: &mut SurMlFile =
        empty_return_safe_eject!(state.get_mut(&file_id_str), "Model not found", Option);
    empty_return_safe_eject!(file.write(&file_path_str));
    state.remove(&file_id_str);
    EmptyReturn::success()
}
