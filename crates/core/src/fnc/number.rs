use std::ops::Deref;

use crate::err::Error;
use crate::expr::Number;
use crate::expr::value::Value;
use anyhow::Result;

pub fn format((val, format): (Number, String)) -> Result<Value> {
    let formatted = match format.deref() {
        "b" => format_binary(val),
        "o" => format_octal(val),
        "x" => format_hexa(val, false),
        "X" => format_hexa(val, true),
        "e" => format_exp(val, false),
        "E" => format_exp(val, true),
		_ => Err(anyhow::Error::new(Error::InvalidArguments {
			name: String::from("number::format"),
			message: String::from(
				"The second argument must be a valid format.",
			),
		})),
    }?;
    Ok(formatted.into())
}

fn format_binary(val: Number) -> Result<String> {
    let Number::Int(val) = val else {
        return Err(anyhow::Error::new(Error::InvalidArguments {
			name: String::from("number::format"),
			message: String::from(
				"The binary formatter only accept integer.",
			),
		}));
    };

    Ok(format!("{:b}", val))
}

fn format_octal(val: Number) -> Result<String> {
    let Number::Int(val) = val else {
        return Err(anyhow::Error::new(Error::InvalidArguments {
			name: String::from("number::format"),
			message: String::from(
				"The octal formatter only accept integer.",
			),
		}));
    };

    Ok(format!("{:o}", val))
}

fn format_hexa(val: Number, uppercase: bool) -> Result<String> {
    let Number::Int(val) = val else {
        return Err(anyhow::Error::new(Error::InvalidArguments {
			name: String::from("number::format"),
			message: String::from(
				"The hexadecimal formatter only accept integer.",
			),
		}));
    };

    if uppercase {
        Ok(format!("{:X}", val))
    } else {
        Ok(format!("{:x}", val))
    }
}

fn format_exp(val: Number, uppercase: bool) -> Result<String> {
    let val = match val {
        Number::Int(val) => Ok(val as f64),
        Number::Float(val) => Ok(val),
        _ => Err(anyhow::Error::new(Error::InvalidArguments {
			name: String::from("number::format"),
			message: String::from(
				"The scientific notation formatter does not accept decimal.",
			),
		})),
    }?;

    if uppercase {
        Ok(format!("{:E}", val))
    } else {
        Ok(format!("{:e}", val))
    }
}