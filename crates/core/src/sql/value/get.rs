use std::collections::BTreeMap;
use std::ops::Deref;

use crate::cnf::MAX_COMPUTATION_DEPTH;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::fnc::idiom;
use crate::sql::edges::Edges;
use crate::sql::field::{Field, Fields};
use crate::sql::id::Id;
use crate::sql::part::{FindRecursionPlan, Next, NextMethod, SplitByRepeatRecurse};
use crate::sql::part::{Part, Skip};
use crate::sql::statements::select::SelectStatement;
use crate::sql::thing::Thing;
use crate::sql::value::{Value, Values};
use crate::sql::{ControlFlow, FlowResult, FlowResultExt as _, Function};
use futures::future::try_join_all;
use reblessive::tree::Stk;

