/*
 * A Runtime is passed around when processing a set of query
 * statements. The Runtime contains any saved parameters and
 * variables set in the SQL, or any pre-defined paramaters which
 * are determined by the authentication / session / environment.
 * Embedded queries, and subqueries will create their own Runtime
 * based off of the parent Runtime, and set their own variables
 * accordingly. Predetermined variables include:
 *
 *    $ENV = "surrealdb.com";
 *
 *    $auth.AL = "KV" / "NS" / "DB" / "SC";
 *    $auth.NS = "";
 *    $auth.DB = "";
 *
 *    $session.id = "";
 *    $session.ip = "";
 *    $session.origin = "app.surrealdb.com";
 *
 */

use crate::ctx::Context;
use std::sync::Arc;

pub type Runtime = Arc<Context>;
