//! Defines the C interface for loading a surml file and getting the meta data around the model.
// Standard library imports
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

// External crate imports
use surrealml_core::storage::surml_file::SurMlFile;

// Local module imports
use crate::state::{generate_unique_id, STATE};

/// Holds the data around the outcome of the load_model function.
///
/// # Fields
/// * `file_id` - The unique identifier for the loaded model.
/// * `name` - The name of the model.
/// * `description` - The description of the model.
/// * `version` - The version of the model.
/// * `error_message` - An error message if the loading failed.
/// * `is_error` - A flag indicating if an error occurred (1 for error, 0 for success).
#[repr(C)]
pub struct FileInfo {
    pub file_id: *mut c_char,
    pub name: *mut c_char,
    pub description: *mut c_char,
    pub version: *mut c_char,
    pub error_message: *mut c_char,
    pub is_error: c_int,
}

/// Frees the memory allocated for the file info.
///
/// # Arguments
/// * `info` - The file info to free.
#[no_mangle]
pub extern "C" fn free_file_info(info: FileInfo) {
    // Free all allocated strings if they are not null
    if !info.file_id.is_null() {
        unsafe { drop(CString::from_raw(info.file_id)) };
    }
    if !info.name.is_null() {
        unsafe { drop(CString::from_raw(info.name)) };
    }
    if !info.description.is_null() {
        unsafe { drop(CString::from_raw(info.description)) };
    }
    if !info.version.is_null() {
        unsafe { drop(CString::from_raw(info.version)) };
    }
    if !info.error_message.is_null() {
        unsafe { drop(CString::from_raw(info.error_message)) };
    }
}

/// Loads a model from a file and returns a unique identifier for the loaded model.
///
/// # Arguments
/// * `file_path_ptr` - A pointer to the file path of the model to load.
///
/// # Returns
/// Meta data around the model and a unique identifier for the loaded model.
#[no_mangle]
pub extern "C" fn load_model(file_path_ptr: *const c_char) -> FileInfo {
    // checking that the file path pointer is not null
    if file_path_ptr.is_null() {
        return FileInfo {
            file_id: std::ptr::null_mut(),
            name: std::ptr::null_mut(),
            description: std::ptr::null_mut(),
            version: std::ptr::null_mut(),
            error_message: CString::new("Received a null pointer for file path")
                .unwrap()
                .into_raw(),
            is_error: 1,
        };
    }

    // Convert the raw C string to a Rust string
    let c_str = unsafe { CStr::from_ptr(file_path_ptr) };

    // convert the CStr into a &str
    let file_path = match c_str.to_str() {
        Ok(rust_str) => rust_str,
        Err(_) => {
            return FileInfo {
                file_id: std::ptr::null_mut(),
                name: std::ptr::null_mut(),
                description: std::ptr::null_mut(),
                version: std::ptr::null_mut(),
                error_message: CString::new("Invalid UTF-8 string received for file path")
                    .unwrap()
                    .into_raw(),
                is_error: 1,
            };
        }
    };

    let file = match SurMlFile::from_file(file_path) {
        Ok(file) => file,
        Err(e) => {
            return FileInfo {
                file_id: std::ptr::null_mut(),
                name: std::ptr::null_mut(),
                description: std::ptr::null_mut(),
                version: std::ptr::null_mut(),
                error_message: CString::new(e.to_string()).unwrap().into_raw(),
                is_error: 1,
            };
        }
    };

    // get the meta data from the file
    let name = file.header.name.to_string();
    let description = file.header.description.to_string();
    let version = file.header.version.to_string();

    // insert the file into the state
    let file_id = generate_unique_id();
    let mut state = STATE.lock().unwrap();
    state.insert(file_id.clone(), file);

    // return the meta data
    let file_id = CString::new(file_id).unwrap();
    let name = CString::new(name).unwrap();
    let description = CString::new(description).unwrap();
    let version = CString::new(version).unwrap();

    FileInfo {
        file_id: file_id.into_raw(),
        name: name.into_raw(),
        description: description.into_raw(),
        version: version.into_raw(),
        error_message: std::ptr::null_mut(),
        is_error: 0,
    }
}
