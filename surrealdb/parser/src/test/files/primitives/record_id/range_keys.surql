a:[1]..[2];
a:{a: 1}..{a: 2};
a:"a".."b";
a:a..b;
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
                                RecordIdKey::Array
                                    Array
                                        .entries: Some
                                            -Expr::Integer
                                                +1
                            .end: Bound::Excluded
                                RecordIdKey::Array
                                    Array
                                        .entries: Some
                                            -Expr::Integer
                                                +2
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Included
                                RecordIdKey::Object
                                    Object
                                        .entries: Some
                                            -ObjectEntry
                                                .key: "a"
                                                .value: Expr::Integer
                                                    +1
                            .end: Bound::Excluded
                                RecordIdKey::Object
                                    Object
                                        .entries: Some
                                            -ObjectEntry
                                                .key: "a"
                                                .value: Expr::Integer
                                                    +2
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Included
                                RecordIdKey::String
                                    StringLit
                                        .text: "a"
                            .end: Bound::Excluded
                                RecordIdKey::String
                                    StringLit
                                        .text: "b"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::Range
                        RecordIdKeyRange
                            .start: Bound::Included
                                RecordIdKey::String
                                    StringLit
                                        .text: "a"
                            .end: Bound::Excluded
                                RecordIdKey::String
                                    StringLit
                                        .text: "b"
*/