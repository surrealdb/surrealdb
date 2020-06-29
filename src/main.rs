#[macro_use]
extern crate failure;
#[macro_use]
extern crate maplit;
#[macro_use]
extern crate log;

mod cli;
mod dbs;
mod err;
mod kvs;
mod sql;
mod web;

fn main() {
	cli::init(); // Initiate the command line
}
