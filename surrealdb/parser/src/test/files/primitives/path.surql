a::b;
a::<1.3.4>::c;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Path
                Path
                    .start: Ident
                        .text: "a"
                    .parts: Some
                        -PathSegment::Ident
                            Ident
                                .text: "b"
        -TopLevelExpr::Expr
            Expr::Path
                Path
                    .start: Ident
                        .text: "a"
                    .parts: Some
                        -PathSegment::Version
                            Version
                                .major: 1
                                .minor: 3
                                .patch: 4
                        -PathSegment::Ident
                            Ident
                                .text: "c"
*/