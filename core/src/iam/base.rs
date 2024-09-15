use base64::alphabet::STANDARD;
use base64::engine::general_purpose::GeneralPurpose;
use base64::engine::general_purpose::GeneralPurposeConfig;
use base64::engine::DecodePaddingMode;

pub use base64::Engine;

pub const BASE64: GeneralPurpose = GeneralPurpose::new(&STANDARD, CONFIG);

pub const CONFIG: GeneralPurposeConfig = GeneralPurposeConfig::new()
	.with_encode_padding(false)
	.with_decode_padding_mode(DecodePaddingMode::Indifferent);
