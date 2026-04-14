DEFINE TABLE foo;
DEFINE TABLE OVERWRITE foo;
DEFINE TABLE IF NOT EXISTS foo;
DEFINE TABLE foo COMMENT "table";
DEFINE TABLE foo DROP COMMENT "table";
DEFINE TABLE foo SCHEMAFULL DROP;
DEFINE TABLE foo SCHEMALESS DROP;
DEFINE TABLE foo AS select * from bar;
DEFINE TABLE foo CHANGEFEED 1s;
DEFINE TABLE foo TYPE NORMAL;
DEFINE TABLE foo TYPE ANY;
DEFINE TABLE foo TYPE RELATION from a|b to c|d ENFORCED;
DEFINE TABLE foo PERMISSIONS FULL;
DEFINE TABLE foo PERMISSIONS NONE;
DEFINE TABLE foo PERMISSIONS FOR select, delete FULL;
DEFINE TABLE foo PERMISSIONS FOR select, delete FULL, FOR create NONE;
DEFINE TABLE foo PERMISSIONS FOR select, delete FULL, FOR create NONE, FOR update WHERE 1 == 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: IfNotExists
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "table"
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "table"
                    .drop: true
                    .schema: None
                    .table_kind: None
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: true
                    .schema: Some
                    Full
                    .table_kind: None
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: true
                    .schema: Some
                    Less
                    .table_kind: None
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: None
                    .changefeed: None
                    .view: Some
                    Select
                        .fields: Fields::List
                                -ListSelector::All
                        .omit: None
                        .only: false
                        .from: 
                            -Expr::Path
                                Path
                                    .start: Ident
                                        .text: "bar"
                                    .parts: None
                        .with_index: None
                        .condition: None
                        .split: None
                        .group: None
                        .order: None
                        .start: None
                        .limit: None
                        .version: None
                        .timeout: None
                        .fetch: None
                        .tempfiles: false
                        .explain: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: None
                    .changefeed: Some
                    ChangeFeed::Base
                        1s
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: Some
                    TableKind::Normal
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: Some
                    TableKind::Any
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: Some
                    TableKind::Relation
                        RelationTable
                            .from: Some
                                -Ident
                                    .text: "a"
                                -Ident
                                    .text: "b"
                            .to: Some
                                -Ident
                                    .text: "c"
                                -Ident
                                    .text: "d"
                            .enforced: true
                    .permission: None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: Some
                    TablePermissions
                        .create: Some
                        Permission::Full
                        .delete: Some
                        Permission::Full
                        .update: Some
                        Permission::Full
                        .select: Some
                        Permission::Full
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: Some
                    TablePermissions
                        .create: Some
                        Permission::None
                        .delete: Some
                        Permission::None
                        .update: Some
                        Permission::None
                        .select: Some
                        Permission::None
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: Some
                    TablePermissions
                        .create: None
                        .delete: Some
                        Permission::Full
                        .update: None
                        .select: Some
                        Permission::Full
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: Some
                    TablePermissions
                        .create: Some
                        Permission::None
                        .delete: Some
                        Permission::Full
                        .update: None
                        .select: Some
                        Permission::Full
                    .changefeed: None
                    .view: None
        -TopLevelExpr::Expr
            Expr::DefineTable
                DefineTable
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "foo"
                            .parts: None
                    .comment: None
                    .drop: false
                    .schema: None
                    .table_kind: None
                    .permission: Some
                    TablePermissions
                        .create: Some
                        Permission::None
                        .delete: Some
                        Permission::Full
                        .update: Some
                        Permission::Where
                            Expr::Binary
                                BinaryExpr
                                    .left: Expr::Integer
                                        +1
                                    .op: ExactEqual
                                    .right: Expr::Integer
                                        +1
                        .select: Some
                        Permission::Full
                    .changefeed: None
                    .view: None
*/