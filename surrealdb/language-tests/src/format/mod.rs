mod indent;
pub use indent::IndentFormatter;
mod progress;
pub use progress::Progress;

/// Simple macro for composing ansi escape codes.
macro_rules! ansi {
    ($first:ident $(, $($t:tt)+)?) => {
        concat!(ansi!(@t $first) $(, ansi!($($t)*))?)
    };

    ($first:literal $(, $($t:tt)+)?) => {
        concat!($first $(, ansi!($($t)*))?)
    };

    (@t up) => {
        "\x1b[1A"
    };
    (@t down) => {
        "\x1b[1B"
    };
    (@t clear_line) => {
        "\x1b[2K"
    };
    (@t clear_after) => {
        "\x1b[0J"
    };
    (@t reset_format) => {
        "\x1b[0m"
    };
    (@t green) => {
        "\x1b[32m"
    };
    (@t red) => {
        "\x1b[31m"
    };
    (@t yellow) => {
        "\x1b[33m"
    };
    (@t blue) => {
        "\x1b[34m"
    };
    (@t bold) => {
        "\x1b[1m"
    };
}

pub(crate) use ansi;
