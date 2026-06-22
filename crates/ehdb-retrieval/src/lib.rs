use std::collections::BTreeMap;

use ehdb_core::{
    ChunkId, DocumentId, EhdbError, EmbeddingModelId, NamespaceName, Result, TenantId,
    TransactionId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    pub id: DocumentId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub source_uri: String,
    pub content_type: String,
    pub created_by: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterDocument {
    pub id: DocumentId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub source_uri: String,
    pub content_type: String,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub id: ChunkId,
    pub document_id: DocumentId,
    pub ordinal: u32,
    pub text: String,
    pub checksum: String,
    pub created_by: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterChunk {
    pub id: ChunkId,
    pub document_id: DocumentId,
    pub ordinal: u32,
    pub text: String,
    pub checksum: String,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Embedding {
    pub chunk_id: ChunkId,
    pub model_id: EmbeddingModelId,
    pub dimensions: usize,
    pub vector: Vec<f32>,
    pub created_by: TransactionId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegisterEmbedding {
    pub chunk_id: ChunkId,
    pub model_id: EmbeddingModelId,
    pub dimensions: usize,
    pub vector: Vec<f32>,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearch {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub model_id: EmbeddingModelId,
    pub query: Vec<f32>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchHit {
    pub chunk: Chunk,
    pub embedding: Embedding,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DocumentKey {
    tenant: TenantId,
    namespace: NamespaceName,
    id: DocumentId,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryRetrievalCatalog {
    documents: BTreeMap<DocumentKey, Document>,
    chunks: BTreeMap<ChunkId, Chunk>,
    embeddings: BTreeMap<(ChunkId, EmbeddingModelId), Embedding>,
}

impl InMemoryRetrievalCatalog {
    pub fn register_document(&mut self, request: RegisterDocument) -> Result<Document> {
        let key = DocumentKey {
            tenant: request.tenant.clone(),
            namespace: request.namespace.clone(),
            id: request.id.clone(),
        };
        if self.documents.contains_key(&key) {
            return Err(EhdbError::AlreadyExists(format!(
                "{}.{}.{}",
                key.tenant, key.namespace, key.id
            )));
        }

        let document = Document {
            id: request.id,
            tenant: request.tenant,
            namespace: request.namespace,
            source_uri: request.source_uri,
            content_type: request.content_type,
            created_by: request.transaction_id,
        };
        self.documents.insert(key, document.clone());
        Ok(document)
    }

    pub fn register_chunk(&mut self, request: RegisterChunk) -> Result<Chunk> {
        self.document_by_id(&request.document_id)?;
        if self.chunks.contains_key(&request.id) {
            return Err(EhdbError::AlreadyExists(request.id.to_string()));
        }

        let chunk = Chunk {
            id: request.id,
            document_id: request.document_id,
            ordinal: request.ordinal,
            text: request.text,
            checksum: request.checksum,
            created_by: request.transaction_id,
        };
        self.chunks.insert(chunk.id.clone(), chunk.clone());
        Ok(chunk)
    }

    pub fn register_embedding(&mut self, request: RegisterEmbedding) -> Result<Embedding> {
        if !self.chunks.contains_key(&request.chunk_id) {
            return Err(EhdbError::NotFound(request.chunk_id.to_string()));
        }
        if request.dimensions == 0 || request.vector.len() != request.dimensions {
            return Err(EhdbError::InvalidState(format!(
                "embedding dimensions {} do not match vector length {}",
                request.dimensions,
                request.vector.len()
            )));
        }
        validate_vector("embedding vector", &request.vector)?;

        let key = (request.chunk_id.clone(), request.model_id.clone());
        if self.embeddings.contains_key(&key) {
            return Err(EhdbError::AlreadyExists(format!(
                "{}.{}",
                request.chunk_id, request.model_id
            )));
        }

        let embedding = Embedding {
            chunk_id: request.chunk_id,
            model_id: request.model_id,
            dimensions: request.dimensions,
            vector: request.vector,
            created_by: request.transaction_id,
        };
        self.embeddings.insert(key, embedding.clone());
        Ok(embedding)
    }

    pub fn chunks_for_document(&self, document_id: &DocumentId) -> Vec<Chunk> {
        let mut chunks: Vec<_> = self
            .chunks
            .values()
            .filter(|chunk| &chunk.document_id == document_id)
            .cloned()
            .collect();
        chunks.sort_by_key(|chunk| chunk.ordinal);
        chunks
    }

    pub fn find_chunks_containing(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        needle: &str,
    ) -> Vec<Chunk> {
        let document_ids: Vec<_> = self
            .documents
            .values()
            .filter(|document| &document.tenant == tenant && &document.namespace == namespace)
            .map(|document| document.id.clone())
            .collect();

        let mut chunks: Vec<_> = self
            .chunks
            .values()
            .filter(|chunk| {
                document_ids.contains(&chunk.document_id)
                    && chunk.text.to_lowercase().contains(&needle.to_lowercase())
            })
            .cloned()
            .collect();
        chunks.sort_by_key(|chunk| (chunk.document_id.clone(), chunk.ordinal));
        chunks
    }

    pub fn embedding(&self, chunk_id: &ChunkId, model_id: &EmbeddingModelId) -> Result<&Embedding> {
        self.embeddings
            .get(&(chunk_id.clone(), model_id.clone()))
            .ok_or_else(|| EhdbError::NotFound(format!("{chunk_id}.{model_id}")))
    }

    pub fn search_similar(&self, request: VectorSearch) -> Result<Vec<VectorSearchHit>> {
        if request.limit == 0 {
            return Err(EhdbError::InvalidState(
                "vector search limit must be greater than zero".to_string(),
            ));
        }
        validate_vector("query vector", &request.query)?;

        let query_norm = vector_norm(&request.query);
        let document_ids = self.document_ids_for_scope(&request.tenant, &request.namespace);
        let mut hits: Vec<_> = self
            .embeddings
            .values()
            .filter_map(|embedding| {
                if embedding.model_id != request.model_id
                    || embedding.dimensions != request.query.len()
                {
                    return None;
                }
                let chunk = self.chunks.get(&embedding.chunk_id)?;
                if !document_ids
                    .iter()
                    .any(|document_id| document_id == &chunk.document_id)
                {
                    return None;
                }
                Some(VectorSearchHit {
                    chunk: chunk.clone(),
                    embedding: embedding.clone(),
                    score: cosine_similarity(&request.query, &embedding.vector, query_norm),
                })
            })
            .collect();

        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.chunk.document_id.cmp(&right.chunk.document_id))
                .then_with(|| left.chunk.ordinal.cmp(&right.chunk.ordinal))
                .then_with(|| left.chunk.id.cmp(&right.chunk.id))
        });
        hits.truncate(request.limit);
        Ok(hits)
    }

    fn document_by_id(&self, document_id: &DocumentId) -> Result<&Document> {
        self.documents
            .values()
            .find(|document| &document.id == document_id)
            .ok_or_else(|| EhdbError::NotFound(document_id.to_string()))
    }

    fn document_ids_for_scope(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
    ) -> Vec<DocumentId> {
        self.documents
            .values()
            .filter(|document| &document.tenant == tenant && &document.namespace == namespace)
            .map(|document| document.id.clone())
            .collect()
    }
}

fn validate_vector(label: &str, vector: &[f32]) -> Result<()> {
    if vector.is_empty() {
        return Err(EhdbError::InvalidState(format!(
            "{label} must not be empty"
        )));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(EhdbError::InvalidState(format!(
            "{label} must contain only finite values"
        )));
    }
    if vector_norm_squared(vector) == 0.0 {
        return Err(EhdbError::InvalidState(format!(
            "{label} must not be the zero vector"
        )));
    }
    Ok(())
}

fn vector_norm_squared(vector: &[f32]) -> f32 {
    vector.iter().map(|value| value * value).sum()
}

fn vector_norm(vector: &[f32]) -> f32 {
    vector_norm_squared(vector).sqrt()
}

fn cosine_similarity(left: &[f32], right: &[f32], left_norm: f32) -> f32 {
    let dot: f32 = left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum();
    dot / (left_norm * vector_norm(right))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (TenantId, NamespaceName) {
        (
            TenantId::new("tenant-a").unwrap(),
            NamespaceName::new("knowledge").unwrap(),
        )
    }

    fn register_doc(catalog: &mut InMemoryRetrievalCatalog) -> Document {
        let (tenant, namespace) = ids();
        catalog
            .register_document(RegisterDocument {
                id: DocumentId::new("doc-001").unwrap(),
                tenant,
                namespace,
                source_uri: "artifact://exec-1/report.md".to_string(),
                content_type: "text/markdown".to_string(),
                transaction_id: TransactionId::new("txn-0001").unwrap(),
            })
            .unwrap()
    }

    fn register_scoped_doc(
        catalog: &mut InMemoryRetrievalCatalog,
        tenant: TenantId,
        namespace: NamespaceName,
        id: &str,
        txn: &str,
    ) -> Document {
        catalog
            .register_document(RegisterDocument {
                id: DocumentId::new(id).unwrap(),
                tenant,
                namespace,
                source_uri: format!("artifact://{id}/source.md"),
                content_type: "text/markdown".to_string(),
                transaction_id: TransactionId::new(txn).unwrap(),
            })
            .unwrap()
    }

    fn register_chunk(
        catalog: &mut InMemoryRetrievalCatalog,
        document_id: DocumentId,
        id: &str,
        ordinal: u32,
        txn: &str,
    ) -> Chunk {
        catalog
            .register_chunk(RegisterChunk {
                id: ChunkId::new(id).unwrap(),
                document_id,
                ordinal,
                text: format!("retrieval chunk {id}"),
                checksum: format!("sha256-{id}"),
                transaction_id: TransactionId::new(txn).unwrap(),
            })
            .unwrap()
    }

    fn register_embedding(
        catalog: &mut InMemoryRetrievalCatalog,
        chunk_id: ChunkId,
        model_id: &EmbeddingModelId,
        vector: Vec<f32>,
        txn: &str,
    ) -> Embedding {
        catalog
            .register_embedding(RegisterEmbedding {
                chunk_id,
                model_id: model_id.clone(),
                dimensions: vector.len(),
                vector,
                transaction_id: TransactionId::new(txn).unwrap(),
            })
            .unwrap()
    }

    #[test]
    fn registers_document_chunks_and_embeddings() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let document = register_doc(&mut catalog);
        let chunk = catalog
            .register_chunk(RegisterChunk {
                id: ChunkId::new("chunk-001").unwrap(),
                document_id: document.id.clone(),
                ordinal: 0,
                text: "NoETL execution lineage belongs with retrieval metadata.".to_string(),
                checksum: "sha256-abc".to_string(),
                transaction_id: TransactionId::new("txn-0002").unwrap(),
            })
            .unwrap();

        let embedding = catalog
            .register_embedding(RegisterEmbedding {
                chunk_id: chunk.id.clone(),
                model_id: EmbeddingModelId::new("text-embedding-3-large").unwrap(),
                dimensions: 3,
                vector: vec![0.1, 0.2, 0.3],
                transaction_id: TransactionId::new("txn-0003").unwrap(),
            })
            .unwrap();

        assert_eq!(catalog.chunks_for_document(&document.id), vec![chunk]);
        assert_eq!(embedding.dimensions, 3);
    }

    #[test]
    fn filters_chunks_by_tenant_namespace_and_text() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let document = register_doc(&mut catalog);
        catalog
            .register_chunk(RegisterChunk {
                id: ChunkId::new("chunk-001").unwrap(),
                document_id: document.id,
                ordinal: 0,
                text: "EHDB replaces a permanent Qdrant dependency.".to_string(),
                checksum: "sha256-def".to_string(),
                transaction_id: TransactionId::new("txn-0002").unwrap(),
            })
            .unwrap();

        let (tenant, namespace) = ids();
        let matches = catalog.find_chunks_containing(&tenant, &namespace, "qdrant");

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, ChunkId::new("chunk-001").unwrap());
    }

    #[test]
    fn vector_search_returns_tenant_scoped_cosine_hits() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let tenant_a = TenantId::new("tenant-a").unwrap();
        let tenant_b = TenantId::new("tenant-b").unwrap();
        let namespace = NamespaceName::new("knowledge").unwrap();
        let model = EmbeddingModelId::new("text-embedding-local").unwrap();
        let other_model = EmbeddingModelId::new("text-embedding-other").unwrap();

        let doc_a = register_scoped_doc(
            &mut catalog,
            tenant_a.clone(),
            namespace.clone(),
            "doc-a",
            "txn-doc-a",
        );
        let doc_b = register_scoped_doc(
            &mut catalog,
            tenant_b,
            namespace.clone(),
            "doc-b",
            "txn-doc-b",
        );
        let close = register_chunk(
            &mut catalog,
            doc_a.id.clone(),
            "chunk-close",
            0,
            "txn-chunk-close",
        );
        let farther = register_chunk(
            &mut catalog,
            doc_a.id,
            "chunk-farther",
            1,
            "txn-chunk-farther",
        );
        let other_tenant = register_chunk(
            &mut catalog,
            doc_b.id,
            "chunk-other-tenant",
            0,
            "txn-chunk-other-tenant",
        );

        register_embedding(
            &mut catalog,
            close.id.clone(),
            &model,
            vec![1.0, 0.0],
            "txn-embedding-close",
        );
        register_embedding(
            &mut catalog,
            farther.id.clone(),
            &model,
            vec![0.5, 0.5],
            "txn-embedding-farther",
        );
        register_embedding(
            &mut catalog,
            other_tenant.id,
            &model,
            vec![1.0, 0.0],
            "txn-embedding-other-tenant",
        );
        register_embedding(
            &mut catalog,
            farther.id.clone(),
            &other_model,
            vec![1.0, 0.0],
            "txn-embedding-other-model",
        );

        let hits = catalog
            .search_similar(VectorSearch {
                tenant: tenant_a,
                namespace,
                model_id: model,
                query: vec![1.0, 0.0],
                limit: 10,
            })
            .unwrap();

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].chunk.id, ChunkId::new("chunk-close").unwrap());
        assert_eq!(hits[0].score, 1.0);
        assert_eq!(hits[1].chunk.id, ChunkId::new("chunk-farther").unwrap());
        assert!(hits[1].score < hits[0].score);
    }

    #[test]
    fn vector_search_applies_limit_and_dimension_compatibility() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let (tenant, namespace) = ids();
        let model = EmbeddingModelId::new("text-embedding-local").unwrap();
        let document = register_scoped_doc(
            &mut catalog,
            tenant.clone(),
            namespace.clone(),
            "doc-limit",
            "txn-doc-limit",
        );
        let dim_two = register_chunk(
            &mut catalog,
            document.id.clone(),
            "chunk-dim-two",
            0,
            "txn-chunk-dim-two",
        );
        let dim_three = register_chunk(
            &mut catalog,
            document.id,
            "chunk-dim-three",
            1,
            "txn-chunk-dim-three",
        );
        register_embedding(
            &mut catalog,
            dim_two.id,
            &model,
            vec![1.0, 0.0],
            "txn-embedding-dim-two",
        );
        register_embedding(
            &mut catalog,
            dim_three.id,
            &model,
            vec![1.0, 0.0, 0.0],
            "txn-embedding-dim-three",
        );

        let hits = catalog
            .search_similar(VectorSearch {
                tenant,
                namespace,
                model_id: model,
                query: vec![1.0, 0.0],
                limit: 1,
            })
            .unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].chunk.id, ChunkId::new("chunk-dim-two").unwrap());
    }

    #[test]
    fn rejects_embedding_dimension_mismatch() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let document = register_doc(&mut catalog);
        let chunk = catalog
            .register_chunk(RegisterChunk {
                id: ChunkId::new("chunk-001").unwrap(),
                document_id: document.id,
                ordinal: 0,
                text: "dimension mismatch".to_string(),
                checksum: "sha256-ghi".to_string(),
                transaction_id: TransactionId::new("txn-0002").unwrap(),
            })
            .unwrap();

        let error = catalog
            .register_embedding(RegisterEmbedding {
                chunk_id: chunk.id,
                model_id: EmbeddingModelId::new("model-a").unwrap(),
                dimensions: 4,
                vector: vec![0.1, 0.2, 0.3],
                transaction_id: TransactionId::new("txn-0003").unwrap(),
            })
            .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    #[test]
    fn rejects_invalid_embedding_and_query_vectors() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let document = register_doc(&mut catalog);
        let chunk = register_chunk(
            &mut catalog,
            document.id,
            "chunk-invalid-vector",
            0,
            "txn-invalid-vector-chunk",
        );
        let model = EmbeddingModelId::new("model-a").unwrap();

        for (vector, transaction_id) in [
            (vec![0.0, 0.0], "txn-zero-vector"),
            (vec![1.0, f32::NAN], "txn-nan-vector"),
            (vec![1.0, f32::INFINITY], "txn-infinite-vector"),
        ] {
            assert!(matches!(
                catalog
                    .register_embedding(RegisterEmbedding {
                        chunk_id: chunk.id.clone(),
                        model_id: EmbeddingModelId::new(transaction_id).unwrap(),
                        dimensions: vector.len(),
                        vector,
                        transaction_id: TransactionId::new(transaction_id).unwrap(),
                    })
                    .unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }

        register_embedding(
            &mut catalog,
            chunk.id,
            &model,
            vec![1.0, 0.0],
            "txn-valid-vector",
        );

        for request in [
            VectorSearch {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: model.clone(),
                query: vec![1.0, 0.0],
                limit: 0,
            },
            VectorSearch {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: model.clone(),
                query: vec![0.0, 0.0],
                limit: 1,
            },
            VectorSearch {
                tenant: TenantId::new("tenant-a").unwrap(),
                namespace: NamespaceName::new("knowledge").unwrap(),
                model_id: model,
                query: vec![f32::NAN, 0.0],
                limit: 1,
            },
        ] {
            assert!(matches!(
                catalog.search_similar(request).unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }
    }

    #[test]
    fn rejects_duplicate_document_chunk_and_embedding() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let document = register_doc(&mut catalog);
        assert!(matches!(
            catalog
                .register_document(RegisterDocument {
                    id: document.id.clone(),
                    tenant: document.tenant.clone(),
                    namespace: document.namespace.clone(),
                    source_uri: document.source_uri.clone(),
                    content_type: document.content_type.clone(),
                    transaction_id: TransactionId::new("txn-0004").unwrap(),
                })
                .unwrap_err(),
            EhdbError::AlreadyExists(_)
        ));

        let chunk = catalog
            .register_chunk(RegisterChunk {
                id: ChunkId::new("chunk-001").unwrap(),
                document_id: document.id,
                ordinal: 0,
                text: "duplicate checks".to_string(),
                checksum: "sha256-jkl".to_string(),
                transaction_id: TransactionId::new("txn-0002").unwrap(),
            })
            .unwrap();
        assert!(matches!(
            catalog
                .register_chunk(RegisterChunk {
                    id: chunk.id.clone(),
                    document_id: chunk.document_id.clone(),
                    ordinal: 1,
                    text: "duplicate chunk".to_string(),
                    checksum: "sha256-mno".to_string(),
                    transaction_id: TransactionId::new("txn-0005").unwrap(),
                })
                .unwrap_err(),
            EhdbError::AlreadyExists(_)
        ));

        let model_id = EmbeddingModelId::new("model-a").unwrap();
        catalog
            .register_embedding(RegisterEmbedding {
                chunk_id: chunk.id.clone(),
                model_id: model_id.clone(),
                dimensions: 2,
                vector: vec![0.1, 0.2],
                transaction_id: TransactionId::new("txn-0003").unwrap(),
            })
            .unwrap();
        assert!(matches!(
            catalog
                .register_embedding(RegisterEmbedding {
                    chunk_id: chunk.id,
                    model_id,
                    dimensions: 2,
                    vector: vec![0.1, 0.2],
                    transaction_id: TransactionId::new("txn-0006").unwrap(),
                })
                .unwrap_err(),
            EhdbError::AlreadyExists(_)
        ));
    }

    #[test]
    fn tenant_namespace_filtering_isolates_results() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let tenant_a = TenantId::new("tenant-a").unwrap();
        let tenant_b = TenantId::new("tenant-b").unwrap();
        let namespace = NamespaceName::new("knowledge").unwrap();

        let doc_a = catalog
            .register_document(RegisterDocument {
                id: DocumentId::new("doc-a").unwrap(),
                tenant: tenant_a.clone(),
                namespace: namespace.clone(),
                source_uri: "artifact://tenant-a/doc.md".to_string(),
                content_type: "text/markdown".to_string(),
                transaction_id: TransactionId::new("txn-0001").unwrap(),
            })
            .unwrap();
        let doc_b = catalog
            .register_document(RegisterDocument {
                id: DocumentId::new("doc-b").unwrap(),
                tenant: tenant_b.clone(),
                namespace: namespace.clone(),
                source_uri: "artifact://tenant-b/doc.md".to_string(),
                content_type: "text/markdown".to_string(),
                transaction_id: TransactionId::new("txn-0002").unwrap(),
            })
            .unwrap();

        for (id, document_id, txn) in [
            ("chunk-a", doc_a.id, "txn-0003"),
            ("chunk-b", doc_b.id, "txn-0004"),
        ] {
            catalog
                .register_chunk(RegisterChunk {
                    id: ChunkId::new(id).unwrap(),
                    document_id,
                    ordinal: 0,
                    text: "shared retrieval phrase".to_string(),
                    checksum: format!("sha256-{id}"),
                    transaction_id: TransactionId::new(txn).unwrap(),
                })
                .unwrap();
        }

        let matches = catalog.find_chunks_containing(&tenant_a, &namespace, "retrieval");

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, ChunkId::new("chunk-a").unwrap());
    }

    #[test]
    fn missing_document_or_embedding_returns_not_found() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let missing_doc = DocumentId::new("missing-doc").unwrap();
        let missing_chunk = ChunkId::new("missing-chunk").unwrap();
        let model = EmbeddingModelId::new("model-a").unwrap();

        assert!(matches!(
            catalog
                .register_chunk(RegisterChunk {
                    id: ChunkId::new("chunk-001").unwrap(),
                    document_id: missing_doc,
                    ordinal: 0,
                    text: "missing document".to_string(),
                    checksum: "sha256-missing".to_string(),
                    transaction_id: TransactionId::new("txn-0001").unwrap(),
                })
                .unwrap_err(),
            EhdbError::NotFound(_)
        ));
        assert!(matches!(
            catalog.embedding(&missing_chunk, &model).unwrap_err(),
            EhdbError::NotFound(_)
        ));
    }
}
