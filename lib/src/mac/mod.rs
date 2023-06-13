/// Converts some text into a new line byte string
macro_rules! bytes {
	($expression:expr) => {
		format!("{}\n", $expression).into_bytes()
	};
}

/// Creates a new b-tree map of key-value pairs
macro_rules! map {
    ($($k:expr => $v:expr),* $(,)?) => {{
        ::std::collections::BTreeMap::from([
            $(($k, $v),)+
        ])
    }};
}

/// Matches on a specific config environment
macro_rules! get_cfg {
	($i:ident : $($s:expr),+) => (
		let $i = || { $( if cfg!($i=$s) { return $s; } );+ "unknown"};
	)
}

#[cfg(feature = "scripting")]
macro_rules! throw {
	($ctx:expr,$e:ident) => {
		js::Exception::from_message($ctx, &$e.to_string())
			.map(js::Exception::throw)
			.unwrap_or(js::Error::Exception)
		/*
		 * TODO: add line and file back in later
		js::Error::Exception {
			line: line!() as i32,
			message: $e.to_string(),
			file: file!().to_owned(),
			stack: "".to_owned(),
		}
		*/
	};
	($ctx:expr,$str:expr) => {
		js::Exception::from_message($ctx, &$str)
			.map(js::Exception::throw)
			.unwrap_or(js::Error::Exception)
		/*
		 * TODO: add line and file back in later
		js::Error::Exception {
			line: line!() as i32,
			message: $str.to_owned(),
			file: file!().to_owned(),
			stack: "".to_owned(),
		}
		*/
	};
}
