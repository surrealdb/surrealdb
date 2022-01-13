#[macro_use]
extern crate log;

#[macro_use]
mod mac;

mod cli;
mod cnf;
mod ctx;
mod dbs;
mod doc;
mod err;
mod fnc;
mod key;
mod kvs;
mod sql;
mod web;

fn main() {
	cli::init(); // Initiate the command line
}
