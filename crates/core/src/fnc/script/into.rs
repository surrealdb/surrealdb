use js::{
	Array, BigInt, Class, Ctx, Error, Exception, FromIteratorJs as _, IntoJs, Null, Object,
	TypedArray, Undefined,
};
use rust_decimal::prelude::ToPrimitive;

use super::classes;
use crate::val::{Geometry, Number, Value};

const F64_INT_MAX: i64 = ((1u64 << f64::MANTISSA_DIGITS) - 1) as i64;
const F64_INT_MIN: i64 = -F64_INT_MAX - 1;

impl<'js> IntoJs<'js> for Value {
	fn into_js(self, ctx: &Ctx<'js>) -> Result<js::Value<'js>, Error> {
		(&self).into_js(ctx)
	}
}

impl<'js> IntoJs<'js> for &Value {
	fn into_js(self, ctx: &Ctx<'js>) -> Result<js::Value<'js>, Error> {
		match *self {
			Value::Null => Null.into_js(ctx),
			Value::None => Undefined.into_js(ctx),
			Value::Bool(boolean) => Ok(js::Value::new_bool(ctx.clone(), boolean)),
			Value::Strand(ref v) => js::String::from_str(ctx.clone(), v)?.into_js(ctx),
			Value::Number(Number::Int(v)) => {
				if ((i32::MIN as i64)..=(i32::MAX as i64)).contains(&v) {
					Ok(js::Value::new_int(ctx.clone(), v as i32))
				} else if (F64_INT_MIN..=F64_INT_MAX).contains(&v) {
					Ok(js::Value::new_float(ctx.clone(), v as f64))
				} else {
					Ok(js::Value::from(BigInt::from_i64(ctx.clone(), v)?))
				}
			}
			Value::Number(Number::Float(v)) => Ok(js::Value::new_float(ctx.clone(), v)),
			Value::Number(Number::Decimal(v)) => {
				if v.is_integer() {
					if let Some(v) = v.to_i64() {
						if ((i32::MIN as i64)..=(i32::MAX as i64)).contains(&v) {
							Ok(js::Value::new_int(ctx.clone(), v as i32))
						} else if (F64_INT_MIN..=F64_INT_MAX).contains(&v) {
							Ok(js::Value::new_float(ctx.clone(), v as f64))
						} else {
							Ok(js::Value::from(BigInt::from_i64(ctx.clone(), v)?))
						}
					} else {
						Err(Exception::from_message(
							ctx.clone(),
							"Couldn't convert SurrealQL Decimal to JavaScript number",
						)?
						.throw())
					}
				} else if let Ok(v) = v.try_into() {
					Ok(js::Value::new_float(ctx.clone(), v))
				} else {
					// FIXME: Add support for larger numbers if rquickjs ever adds support.
					Err(Exception::from_message(
						ctx.clone(),
						"Couldn't convert SurrealQL Decimal to a JavaScript number",
					)?
					.throw())
				}
			}
			Value::Datetime(ref v) => {
				let date: js::function::Constructor = ctx.globals().get("Date")?;
				date.construct((v.0.timestamp_millis(),))
			}
			Value::RecordId(ref v) => Ok(Class::<classes::record::Record>::instance(
				ctx.clone(),
				classes::record::Record {
					value: v.to_owned(),
				},
			)?
			.into_value()),
			Value::Duration(ref v) => Ok(Class::<classes::duration::Duration>::instance(
				ctx.clone(),
				classes::duration::Duration {
					value: Some(v.to_owned()),
				},
			)?
			.into_value()),
			Value::Uuid(ref v) => Ok(Class::<classes::uuid::Uuid>::instance(
				ctx.clone(),
				classes::uuid::Uuid {
					value: Some(v.to_owned()),
				},
			)?
			.into_value()),
			Value::Array(ref v) => {
				let x = Array::new(ctx.clone())?;
				for (i, v) in v.iter().enumerate() {
					x.set(i, v)?;
				}
				x.into_js(ctx)
			}
			Value::Object(ref v) => {
				let x = Object::new(ctx.clone())?;
				for (k, v) in v.iter() {
					x.set(k, v)?;
				}
				x.into_js(ctx)
			}
			Value::Bytes(ref v) => TypedArray::new_copy(ctx.clone(), v.0.as_slice())?.into_js(ctx),
			Value::Geometry(ref v) => v.into_js(ctx),
			Value::File(ref v) => Ok(Class::<classes::file::File>::instance(
				ctx.clone(),
				classes::file::File {
					value: v.to_owned(),
				},
			)?
			.into_value()),
			_ => Undefined.into_js(ctx),
		}
	}
}

impl<'js> IntoJs<'js> for &Geometry {
	fn into_js(self, ctx: &Ctx<'js>) -> js::Result<js::Value<'js>> {
		let (ty, coords) = match self {
			Geometry::Point(x) => {
				("Point".into_js(ctx)?, Array::from_iter_js(ctx, [x.0.x, x.0.y])?)
			}
			Geometry::Line(x) => {
				let array = Array::new(ctx.clone())?;
				for (idx, c) in x.0.iter().enumerate() {
					let coord = Array::from_iter_js(ctx, [c.x, c.y])?;
					array.set(idx, coord)?;
				}
				("LineString".into_js(ctx)?, array)
			}
			Geometry::Polygon(x) => {
				let coords = Array::new(ctx.clone())?;

				let string = Array::new(ctx.clone())?;
				for (idx, c) in x.exterior().0.iter().enumerate() {
					let coord = Array::from_iter_js(ctx, [c.x, c.y])?;
					string.set(idx, coord)?;
				}

				coords.set(0, string)?;

				for (idx, int) in x.interiors().iter().enumerate() {
					let string = Array::new(ctx.clone())?;
					for (idx, c) in int.0.iter().enumerate() {
						let coord = Array::from_iter_js(ctx, [c.x, c.y])?;
						string.set(idx, coord)?;
					}
					coords.set(idx + 1, string)?;
				}

				("Polygon".into_js(ctx)?, coords)
			}
			Geometry::MultiPoint(x) => {
				let array = Array::new(ctx.clone())?;
				for (idx, c) in x.0.iter().enumerate() {
					let coord = Array::from_iter_js(ctx, [c.x(), c.y()])?;
					array.set(idx, coord)?;
				}
				("MultiPoint".into_js(ctx)?, array)
			}
			Geometry::MultiLine(x) => {
				let lines = Array::new(ctx.clone())?;
				for (idx, l) in x.0.iter().enumerate() {
					let array = Array::new(ctx.clone())?;
					for (idx, c) in l.0.iter().enumerate() {
						let coord = Array::from_iter_js(ctx, [c.x, c.y])?;
						array.set(idx, coord)?;
					}
					lines.set(idx, array)?
				}
				("MultiLineString".into_js(ctx)?, lines)
			}
			Geometry::MultiPolygon(x) => {
				let polygons = Array::new(ctx.clone())?;

				for (idx, p) in x.0.iter().enumerate() {
					let coords = Array::new(ctx.clone())?;

					let string = Array::new(ctx.clone())?;
					for (idx, c) in p.exterior().0.iter().enumerate() {
						let coord = Array::from_iter_js(ctx, [c.x, c.y])?;
						string.set(idx, coord)?;
					}

					coords.set(0, string)?;

					for (idx, int) in p.interiors().iter().enumerate() {
						let string = Array::new(ctx.clone())?;
						for (idx, c) in int.0.iter().enumerate() {
							let coord = Array::from_iter_js(ctx, [c.x, c.y])?;
							string.set(idx, coord)?;
						}
						coords.set(idx + 1, string)?;
					}

					polygons.set(idx, coords)?;
				}
				("MultiPolygon".into_js(ctx)?, polygons)
			}
			Geometry::Collection(x) => {
				let geoms = Array::new(ctx.clone())?;

				for (idx, g) in x.iter().enumerate() {
					let g = g.into_js(ctx)?;
					geoms.set(idx, g)?;
				}

				let object = Object::new(ctx.clone())?;
				object.set("type", "GeometryCollection")?;
				object.set("geometries", geoms)?;
				return Ok(object.into_value());
			}
		};
		let object = Object::new(ctx.clone())?;
		object.set("type", ty)?;
		object.set("coordinates", coords)?;
		Ok(object.into_value())
	}
}
