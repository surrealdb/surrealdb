/// Macro for formatting into a string which can't return an error.
#[macro_export]
macro_rules! swrite {
    ($s:expr, $($t:tt)*) => {
        ::std::fmt::Write::write_fmt($s, format_args!($($t)*)).unwrap()
    };
}

/// Macro for formatting into a string which can't return an error.
#[macro_export]
macro_rules! swriteln {
    ($s:expr, $f:literal $(, $($t:tt)*)?) => {{
        ::std::fmt::Write::write_fmt($s, format_args!($f,$($($t)*)*)).unwrap();
        ::std::fmt::Write::write_str($s, "\n").unwrap();
    }};
}
