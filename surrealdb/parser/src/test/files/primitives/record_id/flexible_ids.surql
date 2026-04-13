a:0123456789abcdef;
a:12345_5678abcdef;
a:0NaN;
a:0Infinity;
a:Infinity;
a:NaN;
a:+10000000000000000000000;
a:-10000000000000000000000;
a:1h;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "0123456789abcdef"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "12345_5678abcdef"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "0NaN"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "0Infinity"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "Infinity"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "NaN"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "+10000000000000000000000"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "-10000000000000000000000"
        -TopLevelExpr::Expr
            Expr::RecordId
                RecordId
                    .name: Ident
                        .text: "a"
                    .key: RecordIdKey::String
                        StringLit
                            .text: "1h"
*/