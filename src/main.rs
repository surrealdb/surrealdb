#[macro_use]
extern crate log;

#[macro_use]
mod mac;

mod cli;
mod cnf;
mod err;
mod net;

fn main() {
	cli::init(); // Initiate the command line
}
