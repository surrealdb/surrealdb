|a:1|;
|`a`:2|;
|`a`:..|;
|`a`:1..|;
|`a`:1>..|;
|`a`:1>..2|;
|`a`:1>..=2|;
|`a`:1..=2|;
|`a`:..=2|;
|`a`:..2|;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Integer
                        +1
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Integer
                        +2
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Range
                        .start: Bound::Unbounded
                        .end: Bound::Unbounded
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Range
                        .start: Bound::Included
                            +1
                        .end: Bound::Unbounded
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Range
                        .start: Bound::Excluded
                            +1
                        .end: Bound::Unbounded
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Range
                        .start: Bound::Excluded
                            +1
                        .end: Bound::Excluded
                            +2
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Range
                        .start: Bound::Excluded
                            +1
                        .end: Bound::Included
                            +2
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Range
                        .start: Bound::Included
                            +1
                        .end: Bound::Included
                            +2
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Range
                        .start: Bound::Unbounded
                        .end: Bound::Included
                            +2
        -TopLevelExpr::Expr
            Expr::Mock
                Mock
                    .name: Ident
                        .text: "a"
                    .kind: MockKind::Range
                        .start: Bound::Unbounded
                        .end: Bound::Excluded
                            +2
*/