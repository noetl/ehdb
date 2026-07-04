use std::collections::BTreeMap;

use ehdb_core::{
    ChunkId, DocumentId, EhdbError, EmbeddingModelId, NamespaceName, Result, TenantId,
    TransactionId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Document {
    pub id: DocumentId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub source_uri: String,
    pub content_type: String,
    pub created_by: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterDocument {
    pub id: DocumentId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub source_uri: String,
    pub content_type: String,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chunk {
    pub id: ChunkId,
    pub document_id: DocumentId,
    pub ordinal: u32,
    pub text: String,
    pub checksum: String,
    pub created_by: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterChunk {
    pub id: ChunkId,
    pub document_id: DocumentId,
    pub ordinal: u32,
    pub text: String,
    pub checksum: String,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Embedding {
    pub chunk_id: ChunkId,
    pub model_id: EmbeddingModelId,
    pub dimensions: usize,
    pub vector: Vec<f32>,
    pub created_by: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterEmbedding {
    pub chunk_id: ChunkId,
    pub model_id: EmbeddingModelId,
    pub dimensions: usize,
    pub vector: Vec<f32>,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorSearch {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub model_id: EmbeddingModelId,
    pub query: Vec<f32>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorSearchHit {
    pub chunk: Chunk,
    pub embedding: Embedding,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextSearch {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub query: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextSearchHit {
    pub chunk: Chunk,
    pub match_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HybridSearch {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub model_id: EmbeddingModelId,
    pub query: Vec<f32>,
    pub text_query: String,
    pub limit: usize,
    pub vector_weight: f32,
    pub text_weight: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HybridSearchHit {
    pub chunk: Chunk,
    pub embedding: Embedding,
    pub vector_score: f32,
    pub text_match_count: usize,
    pub combined_score: f32,
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

    pub fn search_text(&self, request: TextSearch) -> Result<Vec<TextSearchHit>> {
        let query = normalized_text_query(&request.query)?;
        if request.limit == 0 {
            return Err(EhdbError::InvalidState(
                "text search limit must be greater than zero".to_string(),
            ));
        }

        let document_ids = self.document_ids_for_scope(&request.tenant, &request.namespace);
        let mut hits: Vec<_> = self
            .chunks
            .values()
            .filter_map(|chunk| {
                if !document_ids
                    .iter()
                    .any(|document_id| document_id == &chunk.document_id)
                {
                    return None;
                }
                let match_count = count_text_matches(&chunk.text, &query);
                if match_count == 0 {
                    return None;
                }
                Some(TextSearchHit {
                    chunk: chunk.clone(),
                    match_count,
                })
            })
            .collect();

        hits.sort_by(|left, right| {
            right
                .match_count
                .cmp(&left.match_count)
                .then_with(|| left.chunk.document_id.cmp(&right.chunk.document_id))
                .then_with(|| left.chunk.ordinal.cmp(&right.chunk.ordinal))
                .then_with(|| left.chunk.id.cmp(&right.chunk.id))
        });
        hits.truncate(request.limit);
        Ok(hits)
    }

    pub fn search_hybrid(&self, request: HybridSearch) -> Result<Vec<HybridSearchHit>> {
        let text_query = normalized_text_query(&request.text_query)?;
        validate_search_limit("hybrid search", request.limit)?;
        validate_vector("hybrid query vector", &request.query)?;
        validate_hybrid_weights(request.vector_weight, request.text_weight)?;

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
                let vector_score = cosine_similarity(&request.query, &embedding.vector, query_norm);
                let text_match_count = count_text_matches(&chunk.text, &text_query);
                let combined_score = request.vector_weight * vector_score
                    + request.text_weight * text_match_count as f32;
                if combined_score == 0.0 {
                    return None;
                }
                Some(HybridSearchHit {
                    chunk: chunk.clone(),
                    embedding: embedding.clone(),
                    vector_score,
                    text_match_count,
                    combined_score,
                })
            })
            .collect();

        hits.sort_by(|left, right| {
            right
                .combined_score
                .total_cmp(&left.combined_score)
                .then_with(|| right.vector_score.total_cmp(&left.vector_score))
                .then_with(|| right.text_match_count.cmp(&left.text_match_count))
                .then_with(|| left.chunk.document_id.cmp(&right.chunk.document_id))
                .then_with(|| left.chunk.ordinal.cmp(&right.chunk.ordinal))
                .then_with(|| left.chunk.id.cmp(&right.chunk.id))
        });
        hits.truncate(request.limit);
        Ok(hits)
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn embedding_count(&self) -> usize {
        self.embeddings.len()
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

fn normalized_text_query(query: &str) -> Result<String> {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return Err(EhdbError::InvalidState(
            "text search query must not be empty".to_string(),
        ));
    }
    Ok(query)
}

fn validate_search_limit(label: &str, limit: usize) -> Result<()> {
    if limit == 0 {
        return Err(EhdbError::InvalidState(format!(
            "{label} limit must be greater than zero"
        )));
    }
    Ok(())
}

fn validate_hybrid_weights(vector_weight: f32, text_weight: f32) -> Result<()> {
    if !vector_weight.is_finite() || !text_weight.is_finite() {
        return Err(EhdbError::InvalidState(
            "hybrid search weights must be finite".to_string(),
        ));
    }
    if vector_weight < 0.0 || text_weight < 0.0 {
        return Err(EhdbError::InvalidState(
            "hybrid search weights must not be negative".to_string(),
        ));
    }
    if vector_weight == 0.0 && text_weight == 0.0 {
        return Err(EhdbError::InvalidState(
            "at least one hybrid search weight must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn count_text_matches(text: &str, normalized_query: &str) -> usize {
    text.to_lowercase().matches(normalized_query).count()
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
        register_chunk_with_text(
            catalog,
            document_id,
            id,
            ordinal,
            format!("retrieval chunk {id}"),
            txn,
        )
    }

    fn register_chunk_with_text(
        catalog: &mut InMemoryRetrievalCatalog,
        document_id: DocumentId,
        id: &str,
        ordinal: u32,
        text: String,
        txn: &str,
    ) -> Chunk {
        catalog
            .register_chunk(RegisterChunk {
                id: ChunkId::new(id).unwrap(),
                document_id,
                ordinal,
                text,
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

    fn add_unknown(mut value: serde_json::Value, pointer: &str) -> serde_json::Value {
        let target = if pointer.is_empty() {
            &mut value
        } else {
            value.pointer_mut(pointer).unwrap()
        };
        target
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_string(), serde_json::json!("field"));
        value
    }

    fn assert_unknown_rejected<T>(value: serde_json::Value, pointer: &str)
    where
        T: for<'de> Deserialize<'de>,
    {
        let value = add_unknown(value, pointer);
        assert!(serde_json::from_value::<T>(value).is_err());
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
    fn retrieval_metadata_json_rejects_unknown_fields() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let (tenant, namespace) = ids();
        let model = EmbeddingModelId::new("text-embedding-local").unwrap();
        let document = register_scoped_doc(
            &mut catalog,
            tenant.clone(),
            namespace.clone(),
            "doc-strict",
            "txn-doc-strict",
        );
        let chunk = register_chunk_with_text(
            &mut catalog,
            document.id.clone(),
            "chunk-strict",
            0,
            "retrieval strict metadata".to_string(),
            "txn-chunk-strict",
        );
        let embedding = register_embedding(
            &mut catalog,
            chunk.id.clone(),
            &model,
            vec![1.0, 0.0],
            "txn-embedding-strict",
        );
        let vector_hit = catalog
            .search_similar(VectorSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                model_id: model.clone(),
                query: vec![1.0, 0.0],
                limit: 1,
            })
            .unwrap()
            .remove(0);
        let text_hit = catalog
            .search_text(TextSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                query: "retrieval".to_string(),
                limit: 1,
            })
            .unwrap()
            .remove(0);
        let hybrid_hit = catalog
            .search_hybrid(HybridSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                model_id: model.clone(),
                query: vec![1.0, 0.0],
                text_query: "retrieval".to_string(),
                limit: 1,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .unwrap()
            .remove(0);

        assert_unknown_rejected::<Document>(serde_json::to_value(&document).unwrap(), "");
        assert_unknown_rejected::<RegisterDocument>(
            serde_json::to_value(RegisterDocument {
                id: DocumentId::new("doc-request").unwrap(),
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                source_uri: "artifact://doc-request/source.md".to_string(),
                content_type: "text/markdown".to_string(),
                transaction_id: TransactionId::new("txn-doc-request").unwrap(),
            })
            .unwrap(),
            "",
        );
        assert_unknown_rejected::<Chunk>(serde_json::to_value(&chunk).unwrap(), "");
        assert_unknown_rejected::<RegisterChunk>(
            serde_json::to_value(RegisterChunk {
                id: ChunkId::new("chunk-request").unwrap(),
                document_id: document.id.clone(),
                ordinal: 1,
                text: "chunk request".to_string(),
                checksum: "sha256-chunk-request".to_string(),
                transaction_id: TransactionId::new("txn-chunk-request").unwrap(),
            })
            .unwrap(),
            "",
        );
        assert_unknown_rejected::<Embedding>(serde_json::to_value(&embedding).unwrap(), "");
        assert_unknown_rejected::<RegisterEmbedding>(
            serde_json::to_value(RegisterEmbedding {
                chunk_id: chunk.id.clone(),
                model_id: model.clone(),
                dimensions: 2,
                vector: vec![1.0, 0.0],
                transaction_id: TransactionId::new("txn-embedding-request").unwrap(),
            })
            .unwrap(),
            "",
        );
        assert_unknown_rejected::<VectorSearch>(
            serde_json::to_value(VectorSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                model_id: model.clone(),
                query: vec![1.0, 0.0],
                limit: 1,
            })
            .unwrap(),
            "",
        );
        assert_unknown_rejected::<VectorSearchHit>(serde_json::to_value(&vector_hit).unwrap(), "");
        assert_unknown_rejected::<VectorSearchHit>(
            serde_json::to_value(&vector_hit).unwrap(),
            "/chunk",
        );
        assert_unknown_rejected::<TextSearch>(
            serde_json::to_value(TextSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                query: "retrieval".to_string(),
                limit: 1,
            })
            .unwrap(),
            "",
        );
        assert_unknown_rejected::<TextSearchHit>(serde_json::to_value(&text_hit).unwrap(), "");
        assert_unknown_rejected::<HybridSearch>(
            serde_json::to_value(HybridSearch {
                tenant,
                namespace,
                model_id: model,
                query: vec![1.0, 0.0],
                text_query: "retrieval".to_string(),
                limit: 1,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .unwrap(),
            "",
        );
        assert_unknown_rejected::<HybridSearchHit>(serde_json::to_value(&hybrid_hit).unwrap(), "");
        assert_unknown_rejected::<HybridSearchHit>(
            serde_json::to_value(&hybrid_hit).unwrap(),
            "/embedding",
        );
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
    fn text_search_returns_tenant_scoped_ranked_hits() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let tenant_a = TenantId::new("tenant-a").unwrap();
        let tenant_b = TenantId::new("tenant-b").unwrap();
        let namespace = NamespaceName::new("knowledge").unwrap();
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
        register_chunk_with_text(
            &mut catalog,
            doc_a.id.clone(),
            "chunk-two-matches",
            0,
            "NoETL retrieval stores retrieval context".to_string(),
            "txn-chunk-two-matches",
        );
        register_chunk_with_text(
            &mut catalog,
            doc_a.id,
            "chunk-one-match",
            1,
            "NoETL retrieval stores lineage context".to_string(),
            "txn-chunk-one-match",
        );
        register_chunk_with_text(
            &mut catalog,
            doc_b.id,
            "chunk-other-tenant",
            0,
            "retrieval retrieval should stay tenant-scoped".to_string(),
            "txn-chunk-other-tenant",
        );

        let hits = catalog
            .search_text(TextSearch {
                tenant: tenant_a,
                namespace,
                query: " Retrieval ".to_string(),
                limit: 10,
            })
            .unwrap();

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].chunk.id, ChunkId::new("chunk-two-matches").unwrap());
        assert_eq!(hits[0].match_count, 2);
        assert_eq!(hits[1].chunk.id, ChunkId::new("chunk-one-match").unwrap());
        assert_eq!(hits[1].match_count, 1);
    }

    #[test]
    fn text_search_applies_limit_empty_results_and_validation() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let (tenant, namespace) = ids();
        let document = register_scoped_doc(
            &mut catalog,
            tenant.clone(),
            namespace.clone(),
            "doc-text",
            "txn-doc-text",
        );
        register_chunk_with_text(
            &mut catalog,
            document.id.clone(),
            "chunk-a",
            0,
            "retrieval alpha".to_string(),
            "txn-chunk-a",
        );
        register_chunk_with_text(
            &mut catalog,
            document.id,
            "chunk-b",
            1,
            "retrieval beta".to_string(),
            "txn-chunk-b",
        );

        let limited = catalog
            .search_text(TextSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                query: "retrieval".to_string(),
                limit: 1,
            })
            .unwrap();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].chunk.id, ChunkId::new("chunk-a").unwrap());

        let empty = catalog
            .search_text(TextSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                query: "missing".to_string(),
                limit: 10,
            })
            .unwrap();
        assert!(empty.is_empty());

        for request in [
            TextSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                query: " ".to_string(),
                limit: 10,
            },
            TextSearch {
                tenant,
                namespace,
                query: "retrieval".to_string(),
                limit: 0,
            },
        ] {
            assert!(matches!(
                catalog.search_text(request).unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }
    }

    #[test]
    fn hybrid_search_combines_vector_and_text_scores() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let tenant = TenantId::new("tenant-a").unwrap();
        let namespace = NamespaceName::new("knowledge").unwrap();
        let model = EmbeddingModelId::new("text-embedding-local").unwrap();
        let document = register_scoped_doc(
            &mut catalog,
            tenant.clone(),
            namespace.clone(),
            "doc-hybrid",
            "txn-doc-hybrid",
        );
        let text_match = register_chunk_with_text(
            &mut catalog,
            document.id.clone(),
            "chunk-text-match",
            0,
            "retrieval retrieval lineage".to_string(),
            "txn-chunk-text-match",
        );
        let vector_match = register_chunk_with_text(
            &mut catalog,
            document.id,
            "chunk-vector-match",
            1,
            "lineage only".to_string(),
            "txn-chunk-vector-match",
        );
        register_embedding(
            &mut catalog,
            text_match.id.clone(),
            &model,
            vec![0.5, 0.5],
            "txn-embedding-text-match",
        );
        register_embedding(
            &mut catalog,
            vector_match.id.clone(),
            &model,
            vec![1.0, 0.0],
            "txn-embedding-vector-match",
        );

        let hits = catalog
            .search_hybrid(HybridSearch {
                tenant,
                namespace,
                model_id: model,
                query: vec![1.0, 0.0],
                text_query: "retrieval".to_string(),
                limit: 10,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .unwrap();

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].chunk.id, ChunkId::new("chunk-text-match").unwrap());
        assert_eq!(hits[0].text_match_count, 2);
        assert!(hits[0].combined_score > hits[1].combined_score);
        assert_eq!(
            hits[1].chunk.id,
            ChunkId::new("chunk-vector-match").unwrap()
        );
        assert_eq!(hits[1].text_match_count, 0);
    }

    #[test]
    fn hybrid_search_scopes_candidates_and_applies_limit() {
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
        let wanted = register_chunk_with_text(
            &mut catalog,
            doc_a.id.clone(),
            "chunk-wanted",
            0,
            "retrieval wanted".to_string(),
            "txn-chunk-wanted",
        );
        let other_dimension = register_chunk_with_text(
            &mut catalog,
            doc_a.id.clone(),
            "chunk-other-dimension",
            1,
            "retrieval wrong dimension".to_string(),
            "txn-chunk-other-dimension",
        );
        let other_tenant = register_chunk_with_text(
            &mut catalog,
            doc_b.id,
            "chunk-other-tenant",
            0,
            "retrieval other tenant".to_string(),
            "txn-chunk-other-tenant",
        );
        register_embedding(
            &mut catalog,
            wanted.id.clone(),
            &model,
            vec![1.0, 0.0],
            "txn-embedding-wanted",
        );
        register_embedding(
            &mut catalog,
            wanted.id,
            &other_model,
            vec![1.0, 0.0],
            "txn-embedding-other-model",
        );
        register_embedding(
            &mut catalog,
            other_dimension.id,
            &model,
            vec![1.0, 0.0, 0.0],
            "txn-embedding-other-dimension",
        );
        register_embedding(
            &mut catalog,
            other_tenant.id,
            &model,
            vec![1.0, 0.0],
            "txn-embedding-other-tenant",
        );

        let hits = catalog
            .search_hybrid(HybridSearch {
                tenant: tenant_a,
                namespace,
                model_id: model,
                query: vec![1.0, 0.0],
                text_query: "retrieval".to_string(),
                limit: 1,
                vector_weight: 1.0,
                text_weight: 1.0,
            })
            .unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].chunk.id, ChunkId::new("chunk-wanted").unwrap());
    }

    #[test]
    fn hybrid_search_handles_empty_results_and_validation() {
        let mut catalog = InMemoryRetrievalCatalog::default();
        let (tenant, namespace) = ids();
        let model = EmbeddingModelId::new("text-embedding-local").unwrap();
        let document = register_scoped_doc(
            &mut catalog,
            tenant.clone(),
            namespace.clone(),
            "doc-hybrid-validation",
            "txn-doc-hybrid-validation",
        );
        let chunk = register_chunk_with_text(
            &mut catalog,
            document.id,
            "chunk-hybrid-validation",
            0,
            "lineage only".to_string(),
            "txn-chunk-hybrid-validation",
        );
        register_embedding(
            &mut catalog,
            chunk.id,
            &model,
            vec![1.0, 0.0],
            "txn-embedding-hybrid-validation",
        );

        let empty = catalog
            .search_hybrid(HybridSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                model_id: model.clone(),
                query: vec![1.0, 0.0],
                text_query: "retrieval".to_string(),
                limit: 10,
                vector_weight: 0.0,
                text_weight: 1.0,
            })
            .unwrap();
        assert!(empty.is_empty());

        for request in [
            HybridSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                model_id: model.clone(),
                query: vec![1.0, 0.0],
                text_query: " ".to_string(),
                limit: 10,
                vector_weight: 1.0,
                text_weight: 1.0,
            },
            HybridSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                model_id: model.clone(),
                query: vec![0.0, 0.0],
                text_query: "retrieval".to_string(),
                limit: 10,
                vector_weight: 1.0,
                text_weight: 1.0,
            },
            HybridSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                model_id: model.clone(),
                query: vec![1.0, 0.0],
                text_query: "retrieval".to_string(),
                limit: 0,
                vector_weight: 1.0,
                text_weight: 1.0,
            },
            HybridSearch {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                model_id: model.clone(),
                query: vec![1.0, 0.0],
                text_query: "retrieval".to_string(),
                limit: 10,
                vector_weight: f32::NAN,
                text_weight: 1.0,
            },
            HybridSearch {
                tenant: tenant.clone(),
                namespace,
                model_id: model,
                query: vec![1.0, 0.0],
                text_query: "retrieval".to_string(),
                limit: 10,
                vector_weight: 0.0,
                text_weight: 0.0,
            },
        ] {
            assert!(matches!(
                catalog.search_hybrid(request).unwrap_err(),
                EhdbError::InvalidState(_)
            ));
        }
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
