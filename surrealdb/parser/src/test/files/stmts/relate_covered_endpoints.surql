RELATE (type::record('person', $i))->knows->(type::record('person', $i * 3));
RELATE (type::record('person', $i))<-knows<-(type::record('person', $i * 3));
RELATE (a:1)->(knows)->(b:2);
RELATE (a:1)->knows->b:2;
RELATE b:2<-knows<-(a:1);
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::Covered
                        Expr::Idiom
                            IdiomExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "type"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "record"
                                .op: IdiomOperator::Call
                                    Some
                                        -Expr::String
                                            StringLit
                                                .text: "person"
                                        -Expr::Param
                                            Param
                                                .text: "i"
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "knows"
                            .parts: None
                    .to: Expr::Covered
                        Expr::Idiom
                            IdiomExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "type"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "record"
                                .op: IdiomOperator::Call
                                    Some
                                        -Expr::String
                                            StringLit
                                                .text: "person"
                                        -Expr::Binary
                                            BinaryExpr
                                                .left: Expr::Param
                                                    Param
                                                        .text: "i"
                                                .op: BinaryOperator::Multiply
                                                .right: Expr::Integer
                                                    +3
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::Covered
                        Expr::Idiom
                            IdiomExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "type"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "record"
                                .op: IdiomOperator::Call
                                    Some
                                        -Expr::String
                                            StringLit
                                                .text: "person"
                                        -Expr::Binary
                                            BinaryExpr
                                                .left: Expr::Param
                                                    Param
                                                        .text: "i"
                                                .op: BinaryOperator::Multiply
                                                .right: Expr::Integer
                                                    +3
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "knows"
                            .parts: None
                    .to: Expr::Covered
                        Expr::Idiom
                            IdiomExpr
                                .left: Expr::Path
                                    Path
                                        .start: Ident
                                            .text: "type"
                                        .parts: Some
                                            -PathSegment::Ident
                                                Ident
                                                    .text: "record"
                                .op: IdiomOperator::Call
                                    Some
                                        -Expr::String
                                            StringLit
                                                .text: "person"
                                        -Expr::Param
                                            Param
                                                .text: "i"
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::Covered
                        Expr::RecordId
                            RecordId
                                .name: Ident
                                    .text: "a"
                                .key: RecordIdKey::Number
                                    1
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "knows"
                            .parts: None
                    .to: Expr::Covered
                        Expr::RecordId
                            RecordId
                                .name: Ident
                                    .text: "b"
                                .key: RecordIdKey::Number
                                    2
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::Covered
                        Expr::RecordId
                            RecordId
                                .name: Ident
                                    .text: "a"
                                .key: RecordIdKey::Number
                                    1
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "knows"
                            .parts: None
                    .to: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "b"
                            .key: RecordIdKey::Number
                                2
                    .data: None
                    .output: None
                    .timeout: None
        -TopLevelExpr::Expr
            Expr::Relate
                Relate
                    .only: false
                    .or_update: false
                    .from: Expr::Covered
                        Expr::RecordId
                            RecordId
                                .name: Ident
                                    .text: "a"
                                .key: RecordIdKey::Number
                                    1
                    .through: Expr::Path
                        Path
                            .start: Ident
                                .text: "knows"
                            .parts: None
                    .to: Expr::RecordId
                        RecordId
                            .name: Ident
                                .text: "b"
                            .key: RecordIdKey::Number
                                2
                    .data: None
                    .output: None
                    .timeout: None
*/