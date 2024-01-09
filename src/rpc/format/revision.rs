use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;

pub fn req(_msg: Message) -> Result<Request, Failure> {
	// This format is not yet implemented
	Err(Failure::INTERNAL_ERROR)
}

pub fn res(_res: Response) -> Result<(usize, Message), Failure> {
	// This format is not yet implemented
	Err(Failure::INTERNAL_ERROR)
}
