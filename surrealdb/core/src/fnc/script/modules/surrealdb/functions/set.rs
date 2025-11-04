use js::prelude::Async;

use super::{fut, run};
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"set",
	"add" => run,
	"all" => fut Async,
	"any" => fut Async,
	"at" => run,
	"complement" => run,
	"contains" => run,
	"difference" => run,
	"filter" => fut Async,
	"find" => fut Async,
	"first" => run,
	"flatten" => run,
	"fold" => fut Async,
	"intersect" => run,
	"is_empty" => run,
	"join" => run,
	"last" => run,
	"len" => run,
	"map" => fut Async,
	"max" => run,
	"min" => run,
	"reduce" => fut Async,
	"remove" => run,
	"slice" => run,
	"union" => run
);
