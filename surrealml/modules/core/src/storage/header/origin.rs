//! Defines the origin of the model in the file.
use crate::errors::error::{SurrealError, SurrealErrorStatus};
use std::fmt;

use super::string_value::StringValue;

const LOCAL: &str = "local";
const SURREAL_DB: &str = "surreal_db";
const NONE: &str = "";

/// Defines the types of origin that are supported.
///
/// # Fields
/// * `Local` - The model was created locally.
/// * `SurrealDb` - The model was created in the surreal database.
/// * `None` - The model has no origin
#[derive(Debug, PartialEq)]
pub enum OriginValue {
    Local(StringValue),
    SurrealDb(StringValue),
    None(StringValue),
}

impl OriginValue {
    /// Creates a new `OriginValue` with no value.
    ///
    /// # Returns
    /// A new `OriginValue` with no value.
    pub fn fresh() -> Self {
        OriginValue::None(StringValue::fresh())
    }

    /// Create a `OriginValue` from a string.
    ///
    /// # Arguments
    /// * `origin` - The origin as a string.
    ///
    /// # Returns
    /// A new `OriginValue`.
    pub fn from_string(origin: String) -> Result<Self, SurrealError> {
        match origin.to_lowercase().as_str() {
            LOCAL => Ok(OriginValue::Local(StringValue::from_string(origin))),
            SURREAL_DB => Ok(OriginValue::SurrealDb(StringValue::from_string(origin))),
            NONE => Ok(OriginValue::None(StringValue::from_string(origin))),
            _ => Err(SurrealError::new(
                format!("invalid origin: {}", origin),
                SurrealErrorStatus::BadRequest,
            )),
        }
    }
}

impl fmt::Display for OriginValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OriginValue::Local(val) | OriginValue::SurrealDb(val) | OriginValue::None(val) => {
                write!(f, "{}", val)
            }
        }
    }
}

/// Defines the origin of the model in the file header.
///
/// # Fields
/// * `origin` - The origin of the model.
/// * `author` - The author of the model.
#[derive(Debug, PartialEq)]
pub struct Origin {
    pub origin: OriginValue,
    pub author: StringValue,
}

impl Origin {
    /// Creates a new origin with no values.
    ///
    /// # Returns
    /// A new origin with no values.
    pub fn fresh() -> Self {
        Origin {
            origin: OriginValue::fresh(),
            author: StringValue::fresh(),
        }
    }

    /// Adds an author to the origin struct.
    ///
    /// # Arguments
    /// * `origin` - The origin to be added.
    pub fn add_author(&mut self, author: String) {
        self.author = StringValue::from_string(author);
    }

    /// Adds an origin to the origin struct.
    ///
    /// # Arguments
    pub fn add_origin(&mut self, origin: String) -> Result<(), SurrealError> {
        self.origin = OriginValue::from_string(origin)?;
        Ok(())
    }

    /// Creates a new origin from a string.
    ///
    /// # Arguments
    /// * `origin` - The origin as a string.
    ///
    /// # Returns
    /// A new origin.
    pub fn from_string(origin: String) -> Result<Self, SurrealError> {
        if origin == *"" {
            return Ok(Origin::fresh());
        }
        let mut split = origin.split("=>");
        let author = split.next().unwrap().to_string();
        let origin = split.next().unwrap().to_string();
        Ok(Origin {
            origin: OriginValue::from_string(origin)?,
            author: StringValue::from_string(author),
        })
    }
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let is_empty_author = self.author.value.is_none();
        let is_empty_origin = matches!(self.origin, OriginValue::None(ref s) if s.value.is_none());

        if is_empty_author && is_empty_origin {
            write!(f, "")
        } else {
            write!(f, "{}=>{}", self.author, self.origin)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_fresh() {
        let origin = Origin::fresh();
        assert_eq!(
            origin,
            Origin {
                origin: OriginValue::fresh(),
                author: StringValue::fresh(),
            }
        );
    }

    #[test]
    fn test_to_string() {
        let origin = Origin {
            origin: OriginValue::from_string("local".to_string()).unwrap(),
            author: StringValue::from_string("author".to_string()),
        };
        assert_eq!(origin.to_string(), "author=>local".to_string());

        let origin = Origin::fresh();
        assert_eq!(origin.to_string(), "".to_string());
    }

    #[test]
    fn test_from_string() {
        let origin = Origin::from_string("author=>local".to_string()).unwrap();
        assert_eq!(
            origin,
            Origin {
                origin: OriginValue::from_string("local".to_string()).unwrap(),
                author: StringValue::from_string("author".to_string()),
            }
        );

        let origin = Origin::from_string("=>local".to_string()).unwrap();

        assert_eq!(None, origin.author.value);
        assert_eq!("local".to_string(), origin.origin.to_string());
    }
}
