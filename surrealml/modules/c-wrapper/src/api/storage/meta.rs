//! Defines the C API interface for interacting with the meta data of a SurML file.
// Standard library imports
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

// External crate imports
use surrealml_core::storage::header::normalisers::wrapper::NormaliserType;

// Local module imports
use crate::state::STATE;
use crate::utils::EmptyReturn;
use crate::{empty_return_safe_eject, process_string_for_empty_return};

/// Adds a name to the SurMlFile struct.
///
/// # Arguments
/// * `file_id` - The unique identifier for the SurMlFile struct.
/// * `model_name` - The name of the model to be added.
#[no_mangle]
pub extern "C" fn add_name(
    file_id_ptr: *const c_char,
    model_name_ptr: *const c_char,
) -> EmptyReturn {
    let file_id = process_string_for_empty_return!(file_id_ptr, "file id");
    let model_name = process_string_for_empty_return!(model_name_ptr, "model name");
    let mut state = STATE.lock().unwrap();
    let wrapped_file = empty_return_safe_eject!(state.get_mut(&file_id), "Model not found", Option);
    wrapped_file.header.add_name(model_name);
    EmptyReturn::success()
}

/// Adds a description to the SurMlFile struct.
///
/// # Arguments
/// * `file_id` - The unique identifier for the SurMlFile struct.
/// * `description` - The description of the model to be added.
#[no_mangle]
pub extern "C" fn add_description(
    file_id_ptr: *const c_char,
    description_ptr: *const c_char,
) -> EmptyReturn {
    let file_id = process_string_for_empty_return!(file_id_ptr, "file id");
    let description = process_string_for_empty_return!(description_ptr, "description");
    let mut state = STATE.lock().unwrap();
    let wrapped_file = empty_return_safe_eject!(state.get_mut(&file_id), "Model not found", Option);
    wrapped_file.header.add_description(description);
    EmptyReturn::success()
}

/// Adds a version to the SurMlFile struct.
///
/// # Arguments
/// * `file_id` - The unique identifier for the SurMlFile struct.
/// * `version` - The version of the model to be added.
#[no_mangle]
pub extern "C" fn add_version(file_id: *const c_char, version: *const c_char) -> EmptyReturn {
    let file_id = process_string_for_empty_return!(file_id, "file id");
    let version = process_string_for_empty_return!(version, "version");
    let mut state = STATE.lock().unwrap();
    let wrapped_file = empty_return_safe_eject!(state.get_mut(&file_id), "Model not found", Option);
    let _ = wrapped_file.header.add_version(version);
    EmptyReturn::success()
}

/// Adds a column to the SurMlFile struct.
///
/// # Arguments
/// * `file_id` - The unique identifier for the SurMlFile struct.
/// * `column_name` - The name of the column to be added.
#[no_mangle]
pub extern "C" fn add_column(file_id: *const c_char, column_name: *const c_char) -> EmptyReturn {
    let file_id = process_string_for_empty_return!(file_id, "file id");
    let column_name = process_string_for_empty_return!(column_name, "column name");
    let mut state = STATE.lock().unwrap();
    let wrapped_file = empty_return_safe_eject!(state.get_mut(&file_id), "Model not found", Option);
    wrapped_file.header.add_column(column_name);
    EmptyReturn::success()
}

/// adds an author to the SurMlFile struct.
///
/// # Arguments
/// * `file_id` - The unique identifier for the SurMlFile struct.
/// * `author` - The author to be added.
#[no_mangle]
pub extern "C" fn add_author(file_id: *const c_char, author: *const c_char) -> EmptyReturn {
    let file_id = process_string_for_empty_return!(file_id, "file id");
    let author = process_string_for_empty_return!(author, "author");
    let mut state = STATE.lock().unwrap();
    let wrapped_file = empty_return_safe_eject!(state.get_mut(&file_id), "Model not found", Option);
    wrapped_file.header.add_author(author);
    EmptyReturn::success()
}

/// Adds an origin of where the model was trained to the SurMlFile struct.
///
/// # Arguments
/// * `file_id` - The unique identifier for the SurMlFile struct.
/// * `origin` - The origin to be added.
#[no_mangle]
pub extern "C" fn add_origin(file_id: *const c_char, origin: *const c_char) -> EmptyReturn {
    let file_id = process_string_for_empty_return!(file_id, "file id");
    let origin = process_string_for_empty_return!(origin, "origin");
    let mut state = STATE.lock().unwrap();
    let wrapped_file = empty_return_safe_eject!(state.get_mut(&file_id), "Model not found", Option);
    let _ = wrapped_file.header.add_origin(origin);
    EmptyReturn::success()
}

/// Adds an engine to the SurMlFile struct.
///
/// # Arguments
/// * `file_id` - The unique identifier for the SurMlFile struct.
/// * `engine` - The engine to be added.
#[no_mangle]
pub extern "C" fn add_engine(file_id: *const c_char, engine: *const c_char) -> EmptyReturn {
    let file_id = process_string_for_empty_return!(file_id, "file id");
    let engine = process_string_for_empty_return!(engine, "engine");
    let mut state = STATE.lock().unwrap();
    let wrapped_file = empty_return_safe_eject!(state.get_mut(&file_id), "Model not found", Option);
    wrapped_file.header.add_engine(engine);
    EmptyReturn::success()
}

/// Adds an output to the SurMlFile struct.
///
/// # Arguments
/// * `file_id` - The unique identifier for the SurMlFile struct.
/// * `output_name` - The name of the output to be added.
/// * `normaliser_label` (Optional) - The label of the normaliser to be applied to the output.
/// * `one` (Optional) - The first parameter of the normaliser.
/// * `two` (Optional) - The second parameter of the normaliser.
#[no_mangle]
pub extern "C" fn add_output(
    file_id_ptr: *const c_char,
    output_name_ptr: *const c_char,
    normaliser_label_ptr: *const c_char,
    one: *const c_char,
    two: *const c_char,
) -> EmptyReturn {
    let file_id = process_string_for_empty_return!(file_id_ptr, "file id");
    let output_name = process_string_for_empty_return!(output_name_ptr, "output name");

    let normaliser_label = if normaliser_label_ptr.is_null() {
        None
    } else {
        Some(process_string_for_empty_return!(
            normaliser_label_ptr,
            "normaliser label"
        ))
    };

    let one = if one.is_null() {
        None
    } else {
        Some(empty_return_safe_eject!(process_string_for_empty_return!(
            one, "one"
        )
        .parse::<f32>()))
    };
    let two = if two.is_null() {
        None
    } else {
        Some(empty_return_safe_eject!(process_string_for_empty_return!(
            two, "two"
        )
        .parse::<f32>()))
    };

    let mut state = STATE.lock().unwrap();
    let file = empty_return_safe_eject!(state.get_mut(&file_id), "Model not found", Option);
    if let Some(normaliser_label) = normaliser_label {
        let normaliser = NormaliserType::new(normaliser_label, one.unwrap(), two.unwrap());
        file.header.add_output(output_name, Some(normaliser));
    } else {
        file.header.add_output(output_name, None);
    }
    EmptyReturn::success()
}

/// Adds a normaliser to the SurMlFile struct.
///
/// # Arguments
/// * `file_id` - The unique identifier for the SurMlFile struct.
/// * `column_name` - The name of the column to which the normaliser will be applied.
/// * `normaliser_label` - The label of the normaliser to be applied to the column.
/// * `one` - The first parameter of the normaliser.
/// * `two` - The second parameter of the normaliser.
#[no_mangle]
pub extern "C" fn add_normaliser(
    file_id_ptr: *const c_char,
    column_name_ptr: *const c_char,
    normaliser_label_ptr: *const c_char,
    one: f32,
    two: f32,
) -> EmptyReturn {
    let file_id = process_string_for_empty_return!(file_id_ptr, "file id");
    let column_name = process_string_for_empty_return!(column_name_ptr, "column name");
    let normaliser_label =
        process_string_for_empty_return!(normaliser_label_ptr, "normaliser label");

    let normaliser = NormaliserType::new(normaliser_label, one, two);
    let mut state = STATE.lock().unwrap();
    let file = empty_return_safe_eject!(state.get_mut(&file_id), "Model not found", Option);
    let _ = file
        .header
        .normalisers
        .add_normaliser(normaliser, column_name, &file.header.keys);
    EmptyReturn::success()
}
