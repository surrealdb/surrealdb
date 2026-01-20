use crate::interfaces::utils::{StringReturn, TokenizerReturn, VecU32Return};
use crate::{
    process_opt_string_for_tokenizer_return, process_string_for_tokenizer_return,
    process_string_for_vecu32_return,
};
use std::ffi::CString;
use std::os::raw::c_char;
use std::slice;
use surrealml_tokenizers::{
    Tokenizer, decode as decode_surrealml, encode as encode_surrealml, load_local_tokenizer,
    load_tokenizer_with_http,
};

#[repr(C)]
pub struct TokenizerHandle {
    tokenizer: Tokenizer,
}

/// Load a tokenizer either from a local bundle or via the Hugging Face HTTP
/// endpoint, and return an owning handle that the caller can use in subsequent
/// `encode`/`decode` calls.
///
/// # Arguments
/// * `model` – Pointer to a **non-null**, NUL-terminated UTF-8 C string
///   specifying the model identifier (e.g. `b"mistralai/Mistral-7B-v0.1\0"`).
/// * `hf_token` – Pointer to a NUL-terminated UTF-8 C string holding a Hugging Face
///   access token **or `NULL`**.  
///   If `hf_token` is `NULL` the tokenizer is loaded from local resources;
///   otherwise it is fetched over HTTP with the supplied token.
///
/// # Returns
/// A [`TokenizerReturn`]:
/// * **Success** – `is_error == 0` and `tokenizer_handle` contains a pointer to
///   a heap-allocated `TokenizerHandle`.  
///   The caller becomes responsible for releasing that handle via the
///   `free_tokenizer` (or similarly named) function exported by this library.
/// * **Error** – `is_error == 1` and `error_message` contains a descriptive
///   C string allocated by this library (also requiring the corresponding
///   free function).
///
/// # Safety
/// * `model` **must be non-null** and point to a valid, NUL-terminated UTF-8
///   string that remains alive for the entire duration of the call.
/// * `hf_token` may be null; if non-null it must satisfy the same UTF-8 and
///   lifetime requirements as `model`.
/// * Neither pointer may be mutated, reallocated, or freed by other threads
///   while this function executes.
/// * The returned `TokenizerHandle` is heap-allocated inside the library; it
///   **must** be freed by the caller using the provided destructor to avoid
///   memory leaks.
///
/// Failure to uphold any of these invariants invokes *undefined behaviour*.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn load_tokenizer(
    model: *const c_char,
    hf_token: *const c_char,
) -> TokenizerReturn {
    let model = process_string_for_tokenizer_return!(model, "model");
    let hf_token = process_opt_string_for_tokenizer_return!(hf_token, "hf_token");

    let tokenizer = match hf_token {
        Some(_) => load_tokenizer_with_http(model, hf_token),
        None => load_local_tokenizer(model),
    };

    match tokenizer {
        Ok(tok) => {
            let handle = Box::new(TokenizerHandle { tokenizer: tok });
            TokenizerReturn::success(Box::into_raw(handle))
        }
        Err(_) => TokenizerReturn::error("Invalid UTF-8 for tokenizer".into()),
    }
}

/// Encode a UTF-8 string into token-IDs with the given tokenizer.
///
/// # Arguments
/// * `tokenizer_handle` – Pointer to a live `TokenizerHandle` created by this library.
/// * `text` – Pointer to a NUL-terminated UTF-8 C string.
///
/// # Returns
/// A [`VecU32Return`] containing the encoded `u32` IDs on success, or an error.
///
/// # Safety
/// * **`tokenizer_handle` must be non-null** and point to a valid `TokenizerHandle` that
///   remains alive for the entire call.
/// * **`text` must be non-null** and point to a valid, NUL-terminated UTF-8 C string.
/// * Neither pointer may be mutated or freed by other threads while the function runs.
/// * The caller is responsible for eventually freeing any heap memory inside the returned
///   `VecU32Return` using the corresponding `free_*` helper provided by this crate.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn encode(
    tokenizer_handle: *mut TokenizerHandle,
    text: *const c_char,
) -> VecU32Return {
    if tokenizer_handle.is_null() {
        return VecU32Return::error(
            "Received null pointer for tokenizer handle in encode fn".into(),
        );
    }
    let text = process_string_for_vecu32_return!(text, "text");
    let tokenizer = unsafe { &(*tokenizer_handle).tokenizer };

    match encode_surrealml(tokenizer, text) {
        Ok(ids) => VecU32Return::success(ids),
        Err(e) => VecU32Return::error(format!("Failed to encode text '{}': {}", text, e)),
    }
}

/// Decode a sequence of token IDs into a UTF-8 string using the given tokenizer.
///
/// # Arguments
/// * `tokenizer_handle` – Pointer to a valid `TokenizerHandle` previously created by
///   this library.
/// * `data_ptr` – Pointer to an array of `u32` token IDs.
/// * `length` – The number of elements in the array at `data_ptr`.
///
/// # Returns
/// A [`StringReturn`] with the following semantics:
/// * **Success** – `is_error == 0` and `string` points to a valid, NUL-terminated C string
///   allocated by this library. The caller becomes responsible for freeing it using the
///   appropriate `free_string` function.
/// * **Error** – `is_error == 1` and `error_message` points to a descriptive C string,
///   also allocated by the library and requiring proper deallocation.
///
/// # Safety
/// * `tokenizer_handle` must be non-null and point to a valid `TokenizerHandle` that was
///   created by this library and is still valid.
/// * `data_ptr` must be non-null and point to an array of `u32` values of at least `length`
///   elements.
/// * Both `tokenizer_handle` and `data_ptr` must remain valid (not mutated or freed) for the
///   duration of this call.
/// * The caller must use the appropriate deallocation function to free the returned `string`
///   or `error_message` to avoid memory leaks.
///
/// Violating any of these conditions results in undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn decode(
    tokenizer_handle: *mut TokenizerHandle,
    data_ptr: *const u32,
    length: usize,
) -> StringReturn {
    if tokenizer_handle.is_null() {
        return StringReturn {
            string: std::ptr::null_mut(),
            is_error: 1,
            error_message: CString::new("Received null pointer for tokenizer handle")
                .unwrap()
                .into_raw(),
        };
    }
    let tokenizer = unsafe { &(*tokenizer_handle).tokenizer };

    if data_ptr.is_null() {
        return StringReturn {
            string: std::ptr::null_mut(),
            is_error: 1,
            error_message: CString::new("Received null pointer for data")
                .unwrap()
                .into_raw(),
        };
    };
    let slice: &[u32] = unsafe { slice::from_raw_parts(data_ptr, length) };

    match decode_surrealml(tokenizer, slice) {
        Ok(decoded_string) => match CString::new(decoded_string) {
            Ok(c_string) => StringReturn {
                string: c_string.into_raw(),
                is_error: 0,
                error_message: std::ptr::null_mut(),
            },
            Err(_) => StringReturn {
                string: std::ptr::null_mut(),
                is_error: 1,
                error_message: CString::new("Failed to create CString from decoded string")
                    .unwrap()
                    .into_raw(),
            },
        },
        Err(_) => StringReturn {
            string: std::ptr::null_mut(),
            is_error: 1,
            error_message: CString::new("Failed to decode data").unwrap().into_raw(),
        },
    }
}
