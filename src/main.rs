#[macro_use]
extern crate maplit;
#[macro_use]
extern crate log;

mod cli;
mod ctx;
mod dbs;
mod doc;
mod err;
mod fnc;
mod kvs;
mod sql;
mod web;

fn main() {
	cli::init(); // Initiate the command line
}
