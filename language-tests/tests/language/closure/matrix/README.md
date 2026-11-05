# Closure matrix

One file per cell. Each cell is a short script that puts a closure, an element
filter or a plain name read in one position and has it read one source: a
`LET`, a `DEFINE PARAM`, a function argument, a `FOR` variable, a field of the
row, `$this`, `$parent`, `$value`, `$after`, a closure argument or the
session. The expectations state what the language answers, so a cell fails
where an engine answers otherwise or declines to run the statement. The
`reason` of each cell names the position and the body it tests.

The cells are maintained by hand. No generator is part of the tree: add a cell
by copying one of its family and changing the statement.

## Reading a file name

`<family>_<position>__<source>__<use>.surql`, with the double underscores
separating the axes. A name without them (`M_`, `X_`, `Y_`, `Z_`) is a single
probe whose name describes the statement.

| Family | Position | Source axis |
|---|---|---|
| `A_` | the position grid: `where`, `proj`, `elemfilter`, `cl_any`, `cl_map`, `cl_filter`, `cl_find`, `cl_fold`, `cl_in_cl`, `ef_in_cl`, `cl_in_ef_where`, `cl_in_ef_seed`, `cl_in_ef_expr`, `meth_map`, `meth_filter`, `meth_any`, `subq_proj`, `subq_in_cl`, `if_cond`, `if_branch`, `if_ef`, `block_cl`, `obj_lit_cl` | `let`, `param`, `fnarg`, `for` |
| `R_` | the positions that read a field of the row (`where_cl`, `proj_filter`, `proj_meth`, `proj_ef`, `where_cl_in_ef`, `proj_cl_in_cl`) | `rowfield` |
| `S_` | `$this` in an element filter, a closure body or a `WHERE` | `$this` |
| `P_` | a subquery reading `$parent`, and a closure argument or `LET` read inside a subquery | `$parent`, closure argument |
| `O_` | a closure nested in a closure reading the outer closure's argument | `outerarg` |
| `L_` | `FOR` loops: the loop variable captured by a closure, an element filter, a subquery, a nested loop; closures stored in or collected across iterations | `forvar` |
| `I_` | `IF` statements (not expressions) holding a closure or an element filter | `let`, `param` |
| `G_` | graph traversals: a `WHERE` or element filter on an edge step, recursion, a projection of an edge field | `let`, `param`, `$parent`, row field |
| `D_` | `DEFINE FIELD` clauses (`VALUE`, `COMPUTED`, `DEFAULT`, `ASSERT`) and the names `$value`, `$input`, `$after` read there | `let`, `param`, `rowfield`, `this`, `value`, `input` |
| `E_` | `DEFINE EVENT` bodies and `WHEN` conditions | `let`, `param`, `after` |
| `U_` | `DEFINE FUNCTION` bodies and calls of functions from `WHERE` and closures | `fnarg`, `param`, `let_hidden` (a caller `LET` the body must not see) |
| `M_` | one-off statements: stored closures and shadowing, blocks, write statements, `SELECT` clauses, index-backed `WHERE`, builtins that take closures | none |
| `Q_` | plain `LET` or `DEFINE PARAM` reads in stored clauses and function bodies, mostly with no closure, the controls for the cells that read a hidden caller `LET` | none |
| `X_` | boundary probes: a captured closure in each part of the idiom and statement grammar | none |
| `Y_` | a method called on a receiver path (`$o.a.filter(..)`, `$value.a.filter(..)`, `$after.a.filter(..)`) | none |
| `Z_` | element filters over an array of objects whose closure reads a field of the element | none |
| `PERM_` | a record user reading through a table, field or write permission that holds a closure; the setup is in `perm/<name>_import.surql` | none |

The use axis of `A_` cells is the shape of the body that reads the source:
`cmp` (a comparison), `fn` (a builtin call), `nest` (a nested closure), `idx`
(an index argument), `arr` (an array index), `obj` (an object field). A
`_hidden` source is a caller `LET` that the frame of a stored clause or
function does not see.

## Where the expectations come from

Each expectation is what the language answers for that statement. Where a
statement reads a caller `LET` inside a stored clause, a function or a
permission (`D_field_*__let__cmp`, `E_event_*__let__cmp`, `U_*__let_hidden__cmp`,
the `Q_` controls, `PERM_select_{cl,cmp}_let_hidden`, `Y_mcall_value_field_cap`), the clause's
frame does not see the `LET`, and the cell expects the name to read as unbound.
A closure written in an element filter reads the element's fields, not the
row it is invoked over (`R_where_cl_in_ef__rowfield__{cmp,nest}`,
`R_proj_ef__rowfield__nest`). A table's rows come with explicit record ids so
that edge order is stable.
