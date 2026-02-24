a:1..2;
a:1>..2;
a:1>..=2;
a:1..=2;
a:..=2;
a:..2;
a:1..;
a:1>..;
a:..;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Included
                                RecordIdKey::Number
                                    +1
                            .end: Bound::Excluded
                                RecordIdKey::Number
                                    +2
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Excluded
                                RecordIdKey::Number
                                    +1
                            .end: Bound::Excluded
                                RecordIdKey::Number
                                    +2
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Excluded
                                RecordIdKey::Number
                                    +1
                            .end: Bound::Included
                                RecordIdKey::Number
                                    +2
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Included
                                RecordIdKey::Number
                                    +1
                            .end: Bound::Included
                                RecordIdKey::Number
                                    +2
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Unbounded
                            .end: Bound::Included
                                RecordIdKey::Number
                                    +2
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Unbounded
                            .end: Bound::Excluded
                                RecordIdKey::Number
                                    +2
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Included
                                RecordIdKey::Number
                                    +1
                            .end: Bound::Unbounded
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Excluded
                                RecordIdKey::Number
                                    +1
                            .end: Bound::Unbounded
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Unbounded
                            .end: Bound::Unbounded
*/