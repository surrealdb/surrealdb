DEFINE INDEX pts_embedding_diskann ON pts
	FIELDS embedding DISKANN
		DIMENSION 4 DEGREE 16 L_BUILD 64 ALPHA 1.2 TYPE F32 DIST EUCLIDEAN;
/* ===== result =====
Query
    .exprs: Some
        -TopLevelExpr::Expr
            Expr::DefineIndex
                DefineIndex
                    .kind: Create
                    .name: Expr::Path
                        Path
                            .start: Ident
                                .text: "pts_embedding_diskann"
                            .parts: None
                    .table: Expr::Path
                        Path
                            .start: Ident
                                .text: "pts"
                            .parts: None
                    .fields: Some
                        -Expr::Path
                            Path
                                .start: Ident
                                    .text: "embedding"
                                .parts: None
                    .comment: None
                    .index: Some
                    Index::DiskAnn
                        DiskAnnIndex
                            .dimension: +4
                            .distance: Some
                            Distance::Euclidean
                            .ty: Some
                            F32
                            .degree: Some
                            +16
                            .l_build: Some
                            +64
                            .alpha: Some
                            1.2
                            .use_hashed_vector: false
                    .concurrently: false
*/