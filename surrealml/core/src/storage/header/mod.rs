//! Handles the loading, saving, and utilisation of all the data in the header of the model file.
pub mod engine;
pub mod input_dims;
pub mod keys;
pub mod normalisers;
pub mod origin;
pub mod output;
pub mod string_value;
pub mod version;

use engine::Engine;
use input_dims::InputDims;
use keys::KeyBindings;
use normalisers::NormaliserMap;
use normalisers::wrapper::NormaliserType;
use origin::Origin;
use output::Output;
use string_value::StringValue;
use version::Version;

use crate::errors::error::{SurrealError, SurrealErrorStatus};
use crate::safe_eject;

/// The header of the model file.
///
/// # Fields
/// * `keys` - The key bindings where the order of the input columns is stored.
/// * `normalisers` - The normalisers where the normalisation functions are stored per column if
///   there are any.
/// * `output` - The output where the output column name and normaliser are stored if there are any.
/// * `name` - The name of the model.
/// * `version` - The version of the model.
/// * `description` - The description of the model.
/// * `engine` - The engine of the model (could be native or pytorch).
/// * `origin` - The origin of the model which is where the model was created and who the author is.
#[derive(Debug, PartialEq)]
pub struct Header {
	pub keys: KeyBindings,
	pub normalisers: NormaliserMap,
	pub output: Output,
	pub name: StringValue,
	pub version: Version,
	pub description: StringValue,
	pub engine: Engine,
	pub origin: Origin,
	pub input_dims: InputDims,
}

impl Header {
	/// Creates a new header with no columns or normalisers.
	///
	/// # Returns
	/// A new header with no columns or normalisers.
	pub fn fresh() -> Self {
		Header {
			keys: KeyBindings::fresh(),
			normalisers: NormaliserMap::fresh(),
			output: Output::fresh(),
			name: StringValue::fresh(),
			version: Version::fresh(),
			description: StringValue::fresh(),
			engine: Engine::fresh(),
			origin: Origin::fresh(),
			input_dims: InputDims::fresh(),
		}
	}

	/// Adds a model name to the `self.name` field.
	///
	/// # Arguments
	/// * `model_name` - The name of the model to be added.
	pub fn add_name(&mut self, model_name: String) {
		self.name = StringValue::from_string(model_name);
	}

	/// Adds a version to the `self.version` field.
	///
	/// # Arguments
	/// * `version` - The version to be added.
	pub fn add_version(&mut self, version: String) -> Result<(), SurrealError> {
		self.version = Version::from_string(version)?;
		Ok(())
	}

	/// Adds a description to the `self.description` field.
	///
	/// # Arguments
	/// * `description` - The description to be added.
	pub fn add_description(&mut self, description: String) {
		self.description = StringValue::from_string(description);
	}

	/// Adds a column name to the `self.keys` field. It must be noted that the order in which the
	/// columns are added is the order in which they will be expected in the input data. We can do
	/// this with the followng example:
	///
	/// # Arguments
	/// * `column_name` - The name of the column to be added.
	pub fn add_column(&mut self, column_name: String) {
		self.keys.add_column(column_name);
	}

	/// Adds a normaliser to the `self.normalisers` field.
	///
	/// # Arguments
	/// * `column_name` - The name of the column to which the normaliser will be applied.
	/// * `normaliser` - The normaliser to be applied to the column.
	pub fn add_normaliser(
		&mut self,
		column_name: String,
		normaliser: NormaliserType,
	) -> Result<(), SurrealError> {
		self.normalisers.add_normaliser(normaliser, column_name, &self.keys)?;
		Ok(())
	}

	/// Gets the normaliser for a given column name.
	///
	/// # Arguments
	/// * `column_name` - The name of the column to which the normaliser will be applied.
	///
	/// # Returns
	/// The normaliser for the given column name.
	pub fn get_normaliser(
		&self,
		column_name: &String,
	) -> Result<Option<&NormaliserType>, SurrealError> {
		self.normalisers.get_normaliser(column_name.to_string(), &self.keys)
	}

	/// Adds an output column to the `self.output` field.
	///
	/// # Arguments
	/// * `column_name` - The name of the column to be added.
	/// * `normaliser` - The normaliser to be applied to the column.
	pub fn add_output(&mut self, column_name: String, normaliser: Option<NormaliserType>) {
		self.output.name = Some(column_name);
		self.output.normaliser = normaliser;
	}

	/// Adds an engine to the `self.engine` field.
	///
	/// # Arguments
	/// * `engine` - The engine to be added.
	pub fn add_engine(&mut self, engine: String) {
		self.engine = Engine::from_string(engine);
	}

	/// Adds an author to the `self.origin` field.
	///
	/// # Arguments
	/// * `author` - The author to be added.
	pub fn add_author(&mut self, author: String) {
		self.origin.add_author(author);
	}

	/// Adds an origin to the `self.origin` field.
	///
	/// # Arguments
	/// * `origin` - The origin to be added.
	pub fn add_origin(&mut self, origin: String) -> Result<(), SurrealError> {
		self.origin.add_origin(origin)
	}

	/// The standard delimiter used to seperate each field in the header.
	fn delimiter() -> &'static str {
		"//=>"
	}

	/// Constructs the `Header` struct from bytes.
	///
	/// # Arguments
	/// * `data` - The bytes to be converted into a `Header` struct.
	///
	/// # Returns
	/// The `Header` struct.
	pub fn from_bytes(data: Vec<u8>) -> Result<Self, SurrealError> {
		let string_data = safe_eject!(String::from_utf8(data), SurrealErrorStatus::BadRequest);

		let buffer = string_data.split(Self::delimiter()).collect::<Vec<&str>>();

		let keys: KeyBindings = KeyBindings::from_string(buffer.get(1).unwrap_or(&"").to_string());
		let normalisers =
			NormaliserMap::from_string(buffer.get(2).unwrap_or(&"").to_string(), &keys)?;
		let output = Output::from_string(buffer.get(3).unwrap_or(&"").to_string())?;
		let name = StringValue::from_string(buffer.get(4).unwrap_or(&"").to_string());
		let version = Version::from_string(buffer.get(5).unwrap_or(&"").to_string())?;
		let description = StringValue::from_string(buffer.get(6).unwrap_or(&"").to_string());
		let engine = Engine::from_string(buffer.get(7).unwrap_or(&"").to_string());
		let origin = Origin::from_string(buffer.get(8).unwrap_or(&"").to_string())?;
		let input_dims = InputDims::from_string(buffer.get(9).unwrap_or(&"").to_string());
		Ok(Header {
			keys,
			normalisers,
			output,
			name,
			version,
			description,
			engine,
			origin,
			input_dims,
		})
	}

	/// Converts the `Header` struct into bytes.
	///
	/// # Returns
	/// A tuple containing the number of bytes in the header and the bytes themselves.
	pub fn to_bytes(&self) -> (i32, Vec<u8>) {
		let buffer = vec![
			"".to_string(),
			self.keys.to_string(),
			self.normalisers.to_string(),
			self.output.to_string(),
			self.name.to_string(),
			self.version.to_string(),
			self.description.to_string(),
			self.engine.to_string(),
			self.origin.to_string(),
			self.input_dims.to_string(),
			"".to_string(),
		];
		let buffer = buffer.join(Self::delimiter()).into_bytes();
		(buffer.len() as i32, buffer)
	}
}

#[cfg(test)]
mod tests {

	use super::keys::tests::generate_string as generate_key_string;
	use super::normalisers::clipping::Clipping;
	use super::normalisers::linear_scaling::LinearScaling;
	use super::normalisers::log_scale::LogScaling;
	use super::normalisers::tests::generate_string as generate_normaliser_string;
	use super::normalisers::z_score::ZScore;
	use super::*;

	pub fn generate_string() -> String {
		let keys = generate_key_string();
		let normalisers = generate_normaliser_string();
		let output = "g=>linear_scaling(0.0,1.0)".to_string();
		format!(
			"{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
			Header::delimiter(),
			keys,
			Header::delimiter(),
			normalisers,
			Header::delimiter(),
			output,
			Header::delimiter(),
			"test model name",
			Header::delimiter(),
			"0.0.1",
			Header::delimiter(),
			"test description",
			Header::delimiter(),
			Engine::PyTorch,
			Header::delimiter(),
			Origin::from_string("author=>local".to_string()).unwrap(),
			Header::delimiter(),
			InputDims::from_string("1,2".to_string()),
			Header::delimiter(),
		)
	}

	pub fn generate_bytes() -> Vec<u8> {
		generate_string().into_bytes()
	}

	#[test]
	fn test_from_bytes() {
		let header = Header::from_bytes(generate_bytes()).unwrap();

		assert_eq!(header.keys.store.len(), 6);
		assert_eq!(header.keys.reference.len(), 6);
		assert_eq!(header.normalisers.store.len(), 4);

		assert_eq!(header.keys.store[0], "a");
		assert_eq!(header.keys.store[1], "b");
		assert_eq!(header.keys.store[2], "c");
		assert_eq!(header.keys.store[3], "d");
		assert_eq!(header.keys.store[4], "e");
		assert_eq!(header.keys.store[5], "f");
	}

	#[test]
	fn test_empty_header() {
		let string = "//=>//=>//=>//=>//=>//=>//=>//=>//=>".to_string();
		let data = string.as_bytes();
		let header = Header::from_bytes(data.to_vec()).unwrap();

		assert_eq!(header, Header::fresh());

		let string = "".to_string();
		let data = string.as_bytes();
		let header = Header::from_bytes(data.to_vec()).unwrap();

		assert_eq!(header, Header::fresh());
	}

	#[test]
	fn test_to_bytes() {
		let header = Header::from_bytes(generate_bytes()).unwrap();
		let (bytes_num, bytes) = header.to_bytes();
		let string = String::from_utf8(bytes).unwrap();

		// below the integers are correct but there is a difference with the decimal point
		// representation in the string, we can alter this fairly easy and will investigate it
		let expected_string = "//=>a=>b=>c=>d=>e=>f//=>a=>linear_scaling(0,1)//b=>clipping(0,1.5)//c=>log_scaling(10,0)//e=>z_score(0,1)//=>g=>linear_scaling(0,1)//=>test model name//=>0.0.1//=>test description//=>pytorch//=>author=>local//=>1,2//=>".to_string();

		assert_eq!(string, expected_string);
		assert_eq!(bytes_num, expected_string.len() as i32);

		let empty_header = Header::fresh();
		let (bytes_num, bytes) = empty_header.to_bytes();
		let string = String::from_utf8(bytes).unwrap();
		let expected_string = "//=>//=>//=>//=>//=>//=>//=>//=>//=>//=>".to_string();

		assert_eq!(string, expected_string);
		assert_eq!(bytes_num, expected_string.len() as i32);
	}

	#[test]
	fn test_add_column() {
		let mut header = Header::fresh();
		header.add_column("a".to_string());
		header.add_column("b".to_string());
		header.add_column("c".to_string());
		header.add_column("d".to_string());
		header.add_column("e".to_string());
		header.add_column("f".to_string());

		assert_eq!(header.keys.store.len(), 6);
		assert_eq!(header.keys.reference.len(), 6);

		assert_eq!(header.keys.store[0], "a");
		assert_eq!(header.keys.store[1], "b");
		assert_eq!(header.keys.store[2], "c");
		assert_eq!(header.keys.store[3], "d");
		assert_eq!(header.keys.store[4], "e");
		assert_eq!(header.keys.store[5], "f");
	}

	#[test]
	fn test_add_normalizer() {
		let mut header = Header::fresh();
		header.add_column("a".to_string());
		header.add_column("b".to_string());
		header.add_column("c".to_string());
		header.add_column("d".to_string());
		header.add_column("e".to_string());
		header.add_column("f".to_string());

		let _ = header.add_normaliser(
			"a".to_string(),
			NormaliserType::LinearScaling(LinearScaling {
				min: 0.0,
				max: 1.0,
			}),
		);
		let _ = header.add_normaliser(
			"b".to_string(),
			NormaliserType::Clipping(Clipping {
				min: Some(0.0),
				max: Some(1.5),
			}),
		);
		let _ = header.add_normaliser(
			"c".to_string(),
			NormaliserType::LogScaling(LogScaling {
				base: 10.0,
				min: 0.0,
			}),
		);
		let _ = header.add_normaliser(
			"e".to_string(),
			NormaliserType::ZScore(ZScore {
				mean: 0.0,
				std_dev: 1.0,
			}),
		);

		assert_eq!(header.normalisers.store.len(), 4);
		assert_eq!(
			header.normalisers.store[0],
			NormaliserType::LinearScaling(LinearScaling {
				min: 0.0,
				max: 1.0
			})
		);
		assert_eq!(
			header.normalisers.store[1],
			NormaliserType::Clipping(Clipping {
				min: Some(0.0),
				max: Some(1.5)
			})
		);
		assert_eq!(
			header.normalisers.store[2],
			NormaliserType::LogScaling(LogScaling {
				base: 10.0,
				min: 0.0
			})
		);
		assert_eq!(
			header.normalisers.store[3],
			NormaliserType::ZScore(ZScore {
				mean: 0.0,
				std_dev: 1.0
			})
		);
	}
}
