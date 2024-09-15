// RUST_LOG=warn cargo make ci-ws-integration
mod common;

mod ws_integration {
	use super::common;

	/// Tests for the empty protocol format
	mod none {
		// The WebSocket protocol header
		const SERVER: Option<Format> = None;
		// The format to send messages
		const FORMAT: Format = Format::Json;
		// Run all of the common tests
		include!("common/tests.rs");
	}

	/// Tests for the JSON protocol format
	mod json {
		// The WebSocket protocol header
		const SERVER: Option<Format> = Some(Format::Json);
		// The format to send messages
		const FORMAT: Format = Format::Json;
		// Run all of the common tests
		include!("common/tests.rs");
	}

	/// Tests for the CBOR protocol format
	mod cbor {
		// The WebSocket protocol header
		const SERVER: Option<Format> = Some(Format::Cbor);
		// The format to send messages
		const FORMAT: Format = Format::Cbor;
		// Run all of the common tests
		include!("common/tests.rs");
	}

	/// Tests for the MessagePack protocol format
	mod pack {
		// The WebSocket protocol header
		const SERVER: Option<Format> = Some(Format::Pack);
		// The format to send messages
		const FORMAT: Format = Format::Pack;
		// Run all of the common tests
		include!("common/tests.rs");
	}
}
