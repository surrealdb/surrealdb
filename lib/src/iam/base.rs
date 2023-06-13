use base64_lib::alphabet::STANDARD;
use base64_lib::engine::general_purpose::GeneralPurpose;
use base64_lib::engine::general_purpose::GeneralPurposeConfig;
use base64_lib::engine::DecodePaddingMode;

pub use base64_lib::Engine;

pub const BASE64: GeneralPurpose = GeneralPurpose::new(&STANDARD, CONFIG);

pub const CONFIG: GeneralPurposeConfig = GeneralPurposeConfig::new()
	.with_encode_padding(false)
	.with_decode_padding_mode(DecodePaddingMode::Indifferent);
