⟨a⟩::⟨with spaces⟩::⟨!and_other@\⟩stuff\\\u{3948}⟩;
`a`::`with spaces`::`!and_other@\`stuff\\\u{3948}`;
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
                                .text: "with spaces"
                        -PathSegment::Ident
                            Ident
                                .text: "!and_other@⟩stuff\\㥈"
        -TopLevelExpr::Expr
            Expr::Path
                Path
                    .start: Ident
                        .text: "a"
                    .parts: Some
                        -PathSegment::Ident
                            Ident
                                .text: "with spaces"
                        -PathSegment::Ident
                            Ident
                                .text: "!and_other@`stuff\\㥈"
*/