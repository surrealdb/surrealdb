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
