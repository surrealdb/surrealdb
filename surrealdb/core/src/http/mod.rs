#[cfg(not(feature = "http"))]
mod stub;
#[cfg(not(feature = "http"))]
use stub as implementation;

#[cfg(feature = "http")]
mod enabled;
#[cfg(feature = "http")]
use enabled as implementation;

pub use implementation::*;
