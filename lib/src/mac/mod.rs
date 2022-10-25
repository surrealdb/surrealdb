macro_rules! bytes {
	($expression:expr) => {
		format!("{}\n", $expression).into_bytes()
	};
}

macro_rules! map {
    ($($k:expr => $v:expr),* $(,)?) => {{
        let mut m = ::std::collections::BTreeMap::new();
        $(m.insert($k, $v);)+
        m
    }};
}

#[cfg(feature = "scripting")]
macro_rules! throw_js_exception {
	($e:ident) => {
		js::Error::Exception {
			line: line!() as i32,
			message: $e.to_string(),
			file: file!().to_owned(),
			stack: "".to_owned(),
		}
	};
	($str:expr) => {
		js::Error::Exception {
			line: line!() as i32,
			message: $str.to_owned(),
			file: file!().to_owned(),
			stack: "".to_owned(),
		}
	};
}
