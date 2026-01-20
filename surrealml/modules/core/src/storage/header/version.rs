//! Defines the process of managing the version of the `surml` file in the file.
use crate::{
    errors::error::{SurrealError, SurrealErrorStatus},
    safe_eject, safe_eject_option,
};
use std::fmt;

/// The `Version` struct represents the version of the `surml` file.
///
/// # Fields
/// * `one` - The first number in the version.
/// * `two` - The second number in the version.
/// * `three` - The third number in the version.
#[derive(Debug, PartialEq)]
pub struct Version {
    pub one: u8,
    pub two: u8,
    pub three: u8,
}

impl Version {
    /// Creates a new `Version` struct with all zeros.
    ///
    /// # Returns
    /// A new `Version` struct with all zeros.
    pub fn fresh() -> Self {
        Version {
            one: 0,
            two: 0,
            three: 0,
        }
    }

    /// Creates a new `Version` struct from a string.
    ///
    /// # Arguments
    /// * `version` - The version as a string.
    ///
    /// # Returns
    /// A new `Version` struct.
    pub fn from_string(version: String) -> Result<Self, SurrealError> {
        if version == *"" {
            return Ok(Version::fresh());
        }
        let mut split = version.split(".");
        let one_str = safe_eject_option!(split.next());
        let two_str = safe_eject_option!(split.next());
        let three_str = safe_eject_option!(split.next());

        Ok(Version {
            one: safe_eject!(one_str.parse::<u8>(), SurrealErrorStatus::BadRequest),
            two: safe_eject!(two_str.parse::<u8>(), SurrealErrorStatus::BadRequest),
            three: safe_eject!(three_str.parse::<u8>(), SurrealErrorStatus::BadRequest),
        })
    }

    /// Increments the version by one.
    pub fn increment(&mut self) {
        self.three += 1;
        if self.three == 10 {
            self.three = 0;
            self.two += 1;
            if self.two == 10 {
                self.two = 0;
                self.one += 1;
            }
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.one == 0 && self.two == 0 && self.three == 0 {
            write!(f, "")
        } else {
            write!(f, "{}.{}.{}", self.one, self.two, self.three)
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;

    #[test]
    fn test_from_string() {
        let version = Version::from_string("0.0.0".to_string()).unwrap();
        assert_eq!(version.one, 0);
        assert_eq!(version.two, 0);
        assert_eq!(version.three, 0);

        let version = Version::from_string("1.2.3".to_string()).unwrap();
        assert_eq!(version.one, 1);
        assert_eq!(version.two, 2);
        assert_eq!(version.three, 3);
    }

    #[test]
    fn test_to_string() {
        let version = Version {
            one: 0,
            two: 0,
            three: 0,
        };
        assert_eq!(version.to_string(), "");

        let version = Version {
            one: 1,
            two: 2,
            three: 3,
        };
        assert_eq!(version.to_string(), "1.2.3");
    }

    #[test]
    fn test_increment() {
        let mut version = Version {
            one: 0,
            two: 0,
            three: 0,
        };
        version.increment();
        assert_eq!(version.to_string(), "0.0.1");

        let mut version = Version {
            one: 0,
            two: 0,
            three: 9,
        };
        version.increment();
        assert_eq!(version.to_string(), "0.1.0");

        let mut version = Version {
            one: 0,
            two: 9,
            three: 9,
        };
        version.increment();
        assert_eq!(version.to_string(), "1.0.0");

        let mut version = Version {
            one: 9,
            two: 9,
            three: 9,
        };
        version.increment();
        assert_eq!(version.to_string(), "10.0.0");
    }
}
