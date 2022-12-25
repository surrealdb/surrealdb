macro_rules! bytes {
	($expression:expr) => {
		format!("{}\n", $expression).into_bytes()
	};
}

macro_rules! map {
    ($($k:expr => $v:expr),* $(,)?) => {{
        ::std::collections::BTreeMap::from([
            $(($k, $v),)+
        ])
    }};
}

macro_rules! get_cfg {
	($i:ident : $($s:expr),+) => (
		let $i = || { $( if cfg!($i=$s) { return $s; } );+ "unknown"};
	)
}
