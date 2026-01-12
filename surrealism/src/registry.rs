use std::fmt::Debug;
use std::marker::PhantomData;

use anyhow::Result;
use surrealdb_types::SurrealValue;
use surrealism_types::arg::SerializableArg;
use surrealism_types::args::Args;
use surrealism_types::controller::MemoryController;
use surrealism_types::transfer::{Ptr, Transfer};

/// Represents a wrapped function in the Surrealism framework.
///
/// This struct encapsulates a callable function `F` that accepts arguments of type `A`
/// and returns a value of type `R`. It is designed for use in environments like WASM,
/// where functions need to be registered and invoked with type-safe argument handling
/// and memory transfer mechanisms.
///
/// The struct uses `PhantomData` to maintain type information for `A` and `R` without
/// storing actual instances, ensuring compile-time type safety.
///
/// # Type Parameters
/// - `A`: The argument type, which must implement `Args` for argument processing, `Debug` for
///   debugging, and be `'static + Send + Sync` for thread-safety and lifetime.
/// - `R`: The return type, which must implement `Transferrable<CResult<Value>>` for
///   serialization/deserialization, `Debug` for debugging, and be `'static + Send + Sync`.
/// - `F`: The function type, a closure or function pointer that takes `A` and returns `R`, also
///   requiring `'static + Send + Sync`.
///
/// # Fields
/// - `function`: The wrapped callable.
/// - `_phantom`: Phantom data to hold type information for `A` and `R`.
pub struct SurrealismFunction<A, R, F>
where
	A: 'static + Send + Sync + Args + Debug,
	R: 'static + Send + Sync + SurrealValue + Debug,
	F: 'static + Send + Sync + Fn(A) -> Result<R, String>,
{
	function: F,
	_phantom: PhantomData<(A, R)>,
}

impl<A, R, F> SurrealismFunction<A, R, F>
where
	A: 'static + Send + Sync + Args + Debug,
	R: 'static + Send + Sync + SurrealValue + Debug,
	F: 'static + Send + Sync + Fn(A) -> Result<R, String>,
{
	/// Creates a new `SurrealismFunction` from the given callable.
	///
	/// # Parameters
	/// - `function`: The function or closure to wrap.
	///
	/// # Returns
	/// A new instance of `SurrealismFunction`.
	pub fn from(function: F) -> Self {
		Self {
			function,
			_phantom: Default::default(),
		}
	}

	/// Retrieves the kinds (types) of the function's arguments.
	///
	/// This method uses the `Args` trait to determine the SQL kinds for each argument.
	///
	/// # Returns
	/// A vector of `sql::Kind` representing the argument types.
	pub fn args(&self) -> Vec<surrealdb_types::Kind> {
		A::kinds()
	}

	/// Retrieves the kind (type) of the function's return value.
	///
	/// This method uses the `KindOf` trait to determine the SQL kind for the return type.
	///
	/// # Returns
	/// The `sql::Kind` of the return value.
	pub fn returns(&self) -> surrealdb_types::Kind {
		R::kind_of()
	}

	/// Invokes the wrapped function with the provided arguments.
	///
	/// # Parameters
	/// - `args`: The arguments to pass to the function.
	///
	/// # Returns
	/// A `Result` containing the function's return value `R` on success, or an error.
	///
	/// # Errors
	/// Propagates any error from the wrapped function if it returns a `Result`.
	pub fn invoke(&self, args: A) -> Result<Result<R, String>> {
		Ok((self.function)(args))
	}

	/// Prepares the argument kinds for raw transfer over memory.
	///
	/// This method converts the argument kinds into a transferable array and transfers
	/// it using the provided memory controller.
	///
	/// # Parameters
	/// - `controller`: A mutable reference to a `MemoryController` for allocation and transfer.
	///
	/// # Returns
	/// A `Result` containing the transferred array of kinds on success, or an error.
	///
	/// # Errors
	/// - If converting kinds to transferable types fails.
	/// - If transferring the array fails.
	pub fn args_raw(&self, controller: &mut dyn MemoryController) -> Result<Ptr> {
		self.args().transfer(controller)
	}

	/// Prepares the return kind for raw transfer over memory.
	///
	/// This method converts the return kind into a transferable type and transfers
	/// it using the provided memory controller.
	///
	/// # Parameters
	/// - `controller`: A mutable reference to a `MemoryController` for allocation and transfer.
	///
	/// # Returns
	/// A `Result` containing the transferred kind on success, or an error.
	///
	/// # Errors
	/// - If converting the kind to a transferable type fails.
	/// - If transferring the kind fails.
	pub fn returns_raw(&self, controller: &mut dyn MemoryController) -> Result<Ptr> {
		self.returns().transfer(controller)
	}

	/// Invokes the wrapped function using raw transferred arguments.
	///
	/// This method accepts raw transferred arguments, deserializes them into `A`,
	/// invokes the function, and transfers the result back as a `CResult<Value>`.
	///
	/// # Parameters
	/// - `controller`: A mutable reference to a `MemoryController` for allocation and transfer.
	/// - `args`: The transferred array of argument values.
	///
	/// # Returns
	/// A `Result` containing the transferred result on success, or an error.
	///
	/// # Errors
	/// - If accepting/deserializing arguments fails.
	/// - If invoking the function fails.
	/// - If transferring the result fails.
	pub fn invoke_raw(&self, controller: &mut dyn MemoryController, args: Ptr) -> Result<Ptr> {
		let args = A::from_values(Vec::<surrealdb_types::Value>::receive(args, controller)?)?;
		self.invoke(args)?.map(SerializableArg::from).transfer(controller)
	}
}
