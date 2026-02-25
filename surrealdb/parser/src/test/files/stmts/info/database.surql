INFO FOR DATABASE;
INFO FOR db;
INFO FOR db STRUCTURE;
INFO FOR db VERSION 1 STRUCTURE;
INFO FOR db VERSION 1 + 1 STRUCTURE;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Database
                        .version: None
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Database
                        .version: None
                    .structure: false
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Database
                        .version: None
                    .structure: true
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Database
                        .version: Some
                            Expr::Integer
                                +1
                    .structure: true
        -TopLevelExpr::Expr
            Expr::Info
                Info
                    .kind: InfoKind::Database
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