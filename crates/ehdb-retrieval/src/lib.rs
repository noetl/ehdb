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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DocumentKey {
    tenant: TenantId,
    namespace: NamespaceName,
    id: DocumentId,
}

#[derive(Debug, Default)]
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

    fn document_by_id(&self, document_id: &DocumentId) -> Result<&Document> {
        self.documents
            .values()
            .find(|document| &document.id == document_id)
            .ok_or_else(|| EhdbError::NotFound(document_id.to_string()))
    }
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
