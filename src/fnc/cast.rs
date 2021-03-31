use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::literal::Literal;

pub fn run(ctx: &Runtime, name: &String, val: Literal) -> Result<Literal, Error> {
	match name.as_str() {
		"bool" => bool(ctx, val),
		"int" => int(ctx, val),
		"float" => float(ctx, val),
		"string" => string(ctx, val),
		"number" => number(ctx, val),
		"decimal" => number(ctx, val),
		"datetime" => datetime(ctx, val),
		"duration" => duration(ctx, val),
		_ => Ok(val),
	}
}

pub fn bool(ctx: &Runtime, val: Literal) -> Result<Literal, Error> {
	match val.as_bool() {
		true => Ok(Literal::True),
		false => Ok(Literal::False),
	}
}

pub fn int(ctx: &Runtime, val: Literal) -> Result<Literal, Error> {
	Ok(Literal::Int(val.as_int()))
}

pub fn float(ctx: &Runtime, val: Literal) -> Result<Literal, Error> {
	Ok(Literal::Float(val.as_float()))
}

pub fn string(ctx: &Runtime, val: Literal) -> Result<Literal, Error> {
	Ok(Literal::Strand(val.as_strand()))
}

pub fn number(ctx: &Runtime, val: Literal) -> Result<Literal, Error> {
	Ok(Literal::Number(val.as_number()))
}

pub fn datetime(ctx: &Runtime, val: Literal) -> Result<Literal, Error> {
	Ok(Literal::Datetime(val.as_datetime()))
}

pub fn duration(ctx: &Runtime, val: Literal) -> Result<Literal, Error> {
	Ok(Literal::Duration(val.as_duration()))
}
