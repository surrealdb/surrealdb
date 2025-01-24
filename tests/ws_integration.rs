// RUST_LOG=warn cargo make ci-ws-integration
mod common;
mod ws_tests;

mod ws_integration {

	/// Tests for the empty protocol format
	mod none {
		use crate::{common::Format, ws_tests};
		ws_tests::include_tests!(None, Format::Json);
	}

	/// Tests for the JSON protocol format
	mod json {
		use crate::{common::Format, ws_tests};
		ws_tests::include_tests!(Some(Format::Json), Format::Json);
	}

	/// Tests for the CBOR protocol format
	mod cbor {
		use crate::{common::Format, ws_tests};
		ws_tests::include_tests!(Some(Format::Cbor), Format::Cbor);
	}

	/// Tests for the MessagePack protocol format
	mod pack {
		use crate::{common::Format, ws_tests};
		ws_tests::include_tests!(Some(Format::Pack), Format::Pack);
	}
}
