INFO FOR INDEX idx ON foo;
INFO FOR INDEX idx ON TABLE foo;
INFO FOR INDEX 1 + 1 ON TABLE bar + baz;
INFO FOR INDEX 1 + 1 ON TABLE bar + baz STRUCTURE;
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
                                    .text: "idx"
                                .parts: None
                        .table: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Path
                            Path
                                .start: Ident
                                    .text: "idx"
                                .parts: None
                        .table: Expr::Path
                            Path
                                .start: Ident
                                    .text: "foo"
                                .parts: None
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: Add
                                .right: Expr::Integer
                                    +1
                        .table: Expr::Binary
                            BinaryExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "bar"
                                        .parts: None
                                .op: Add
                                .right: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "baz"
                                        .parts: None
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Table
                        .name: Expr::Binary
                            BinaryExpr
                                .left: Expr::Integer
                                    +1
                                .op: Add
                                .right: Expr::Integer
                                    +1
                        .table: Expr::Binary
                            BinaryExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "bar"
                                        .parts: None
                                .op: Add
                                .right: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "baz"
                                        .parts: None
                    .structure: true
*/