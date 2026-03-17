DELETE a;
DELETE a, b;
DELETE ONLY a, b;
DELETE ONLY a, b WITH NOINDEX;
DELETE ONLY a, b WITH INDEX a, b;
DELETE ONLY a, b WITH INDEX a, b WHERE foo == 1;
DELETE ONLY a, b WITH INDEX a, b WHERE foo == 1 TIMEOUT 1s EXPLAIN;
DELETE ONLY a, b WITH INDEX a, b WHERE foo == 1 TIMEOUT 1s EXPLAIN FULL;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Delete
                Delete
                    .only: false
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                    .with_index: None
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Delete
                Delete
                    .only: false
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .with_index: None
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Delete
                Delete
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .with_index: None
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Delete
                Delete
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .with_index: Some
                    WithIndex::None
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Delete
                Delete
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: None
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Delete
                Delete
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: ExactEqual
                            .right: Expr::Integer
                                +1
                    .output: None
                    .timeout: None
                    .explain: None
        -TopLevelExpr::Expr
            Expr::Delete
                Delete
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: ExactEqual
                            .right: Expr::Integer
                                +1
                    .output: None
                    .timeout: Some
                    Expr::Duration
                        1s
                    .explain: Some
                    ExplainClause::Base
        -TopLevelExpr::Expr
            Expr::Delete
                Delete
                    .only: true
                    .targets: 
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "a"
                                .parts: None
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "b"
                                .parts: None
                    .with_index: Some
                    WithIndex::Some
                            -Ident
                                .text: "a"
                            -Ident
                                .text: "b"
                    .condition: Some
                    Expr::Binary
                        BinaryExpr
                            .left: Expr::Path
                                Path
                                    .start: Ident
                                        .text: "foo"
                                    .parts: None
                            .op: ExactEqual
                            .right: Expr::Integer
                                +1
                    .output: None
                    .timeout: Some
                    Expr::Duration
                        1s
                    .explain: Some
                    ExplainClause::Full
*/