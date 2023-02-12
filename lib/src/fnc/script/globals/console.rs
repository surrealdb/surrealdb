#[js::bind(object, public)]
#[quickjs(rename = "console")]
#[allow(clippy::module_inception)]
pub mod console {
	// Specify the imports
	use crate::fnc::script::LOG;
	use crate::sql::value::Value;
	use js::Rest;
	/// Log the input values as INFO
	pub fn log(args: Rest<Value>) {
		info!(
			target: LOG,
			"{}",
			args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" ")
		);
	}
	/// Log the input values as INFO
	pub fn info(args: Rest<Value>) {
		info!(
			target: LOG,
			"{}",
			args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" ")
		);
	}
	/// Log the input values as WARN
	pub fn warn(args: Rest<Value>) {
		warn!(
			target: LOG,
			"{}",
			args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" ")
		);
	}
	/// Log the input values as ERROR
	pub fn error(args: Rest<Value>) {
		error!(
			target: LOG,
			"{}",
			args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" ")
		);
	}
	/// Log the input values as DEBUG
	pub fn debug(args: Rest<Value>) {
		debug!(
			target: LOG,
			"{}",
			args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" ")
		);
	}
	/// Log the input values as TRACE
	pub fn trace(args: Rest<Value>) {
		trace!(
			target: LOG,
			"{}",
			args.iter().map(|v| v.to_raw_string()).collect::<Vec<String>>().join(" ")
		);
	}
}
