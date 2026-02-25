INFO FOR TABLE foo;
INFO FOR TB t;
INFO FOR TB table STRUCTURE;
INFO FOR TB test VERSION 1 + 1 STRUCTURE;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                        .version: None
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "t"
                                .parts: None
                        .version: None
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "table"
                                .parts: None
                        .version: None
                    .structure: true
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "test"
                                .parts: None
                        .version: Some
                            Expr::Binary
                                BinaryExpr
                                    .left: Expr::Integer
                                        +1
                                    .op: Add
                                    .right: Expr::Integer
                                        +1
                    .structure: true
*/