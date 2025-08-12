pub use base64::Engine;
use base64::alphabet::STANDARD;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};

pub const BASE64: GeneralPurpose = GeneralPurpose::new(&STANDARD, CONFIG);

pub const CONFIG: GeneralPurposeConfig = GeneralPurposeConfig::new()
	.with_encode_padding(false)
	.with_decode_padding_mode(DecodePaddingMode::Indifferent);
