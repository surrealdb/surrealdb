DEFINE DATABASE db;
DEFINE DATABASE db COMMENT "db";
DEFINE DATABASE IF NOT EXISTS db;
DEFINE DATABASE OVERWRITE db;
DEFINE DATABASE db STRICT;
DEFINE DATABASE db STRICT CHANGEFEED 1s INCLUDE ORIGINAL;
DEFINE DATABASE "d" + "b";
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineDatabase
                DefineDatabase
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "db"
                            .parts: None
                    .strict: false
                    .changefeed: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineDatabase
                DefineDatabase
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "db"
                            .parts: None
                    .strict: false
                    .changefeed: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "db"
        -TopLevelExpr::Expr
            Expr::DefineDatabase
                DefineDatabase
                    .kind: IfNotExists
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "db"
                            .parts: None
                    .strict: false
                    .changefeed: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineDatabase
                DefineDatabase
                    .kind: Overwrite
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "db"
                            .parts: None
                    .strict: false
                    .changefeed: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineDatabase
                DefineDatabase
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "db"
                            .parts: None
                    .strict: true
                    .changefeed: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineDatabase
                DefineDatabase
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "db"
                            .parts: None
                    .strict: true
                    .changefeed: Some
                    ChangeFeed::WithOriginal
                        1s
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineDatabase
                DefineDatabase
                    .kind: Create
                    .name: Expr::Binary
                        BinaryExpr
                            .left: Expr::String
                                StringLit
                                    .text: "d"
                            .op: Add
                            .right: Expr::String
                                StringLit
                                    .text: "b"
                    .strict: false
                    .changefeed: None
                    .comment: None
*/