DEFINE SEQUENCE na + me;
DEFINE SEQUENCE na + me BATCH 1;
DEFINE SEQUENCE na + me START 1 BATCH 1;
DEFINE SEQUENCE na + me TIMEOUT 1s START 1;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineSequence
                DefineSequence
                    .kind: Create
                    .name: Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "na"
                                    .parts: None
                            .op: Add
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "me"
                                    .parts: None
                    .batch: None
                    .start: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::DefineSequence
                DefineSequence
                    .kind: Create
                    .name: Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "na"
                                    .parts: None
                            .op: Add
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "me"
                                    .parts: None
                    .batch: Some
                    Expr::Integer
                        +1
                    .start: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::DefineSequence
                DefineSequence
                    .kind: Create
                    .name: Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "na"
                                    .parts: None
                            .op: Add
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "me"
                                    .parts: None
                    .batch: Some
                    Expr::Integer
                        +1
                    .start: Some
                    Expr::Integer
                        +1
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::DefineSequence
                DefineSequence
                    .kind: Create
                    .name: Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "na"
                                    .parts: None
                            .op: Add
                            .right: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "me"
                                    .parts: None
                    .batch: None
                    .start: Some
                    Expr::Integer
                        +1
                    .timeout: Some
                    Expr::Duration
                        1s
*/