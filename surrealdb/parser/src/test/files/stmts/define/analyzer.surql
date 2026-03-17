DEFINE ANALYZER ana;
DEFINE ANALYZER ana COMMENT "comment";
DEFINE ANALYZER ana FUNCTION a::b;
DEFINE ANALYZER ana FILTERS ASCII, LOWERCASE, EDGENGRAM(1,2), MAPPER("mapper"), SNOWBALL(nld);
DEFINE ANALYZER ana TOKENIZERS token;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineAnalyzer
                DefineAnalyzer
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "ana"
                            .parts: None
                    .filters: None
                    .tokenizer: None
                    .function: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineAnalyzer
                DefineAnalyzer
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "ana"
                            .parts: None
                    .filters: None
                    .tokenizer: None
                    .function: None
                    .comment: Some
                    Expr::String
                        StringLit
                            .text: "comment"
        -TopLevelExpr::Expr
            Expr::DefineAnalyzer
                DefineAnalyzer
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "ana"
                            .parts: None
                    .filters: None
                    .tokenizer: None
                    .function: Some
                    Path
                        .start: Ident
                            .text: "a"
                        .parts: Some
                            -PathSegment::Ident
                                Ident
                                    .text: "b"
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineAnalyzer
                DefineAnalyzer
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "ana"
                            .parts: None
                    .filters: Some
                        -Filter::Ascii
                        -Filter::Lowercase
                        -Filter::EdgeNgram
                            NgramMapper
                                .min: +1
                                .max: +2
                        -Filter::Mapper
                            StringLit
                                .text: "mapper"
                        -Filter::Snowball
                            Ident
                                .text: "nld"
                    .tokenizer: None
                    .function: None
                    .comment: None
        -TopLevelExpr::Expr
            Expr::DefineAnalyzer
                DefineAnalyzer
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "ana"
                            .parts: None
                    .filters: None
                    .tokenizer: Some
                        -Ident
                            .text: "token"
                    .function: None
                    .comment: None
*/