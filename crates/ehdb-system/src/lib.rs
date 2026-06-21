use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use ehdb_core::{EhdbError, NamespaceName, Result, TenantId, TransactionId};
use ehdb_storage::ObjectPath;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SystemLibraryPath(String);

impl SystemLibraryPath {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 256
            && !value.starts_with('/')
            && !value.contains("..")
            && value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '_' | '-' | '.'));

        if valid {
            Ok(Self(value))
        } else {
            Err(EhdbError::InvalidIdentifier(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SystemLibraryPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EnvironmentName(String);

impl EnvironmentName {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 128
            && value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'));

        if valid {
            Ok(Self(value))
        } else {
            Err(EhdbError::InvalidIdentifier(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EnvironmentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReleaseChannel(String);

impl ReleaseChannel {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 64
            && value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'));

        if valid {
            Ok(Self(value))
        } else {
            Err(EhdbError::InvalidIdentifier(value))
        }
    }

    pub fn stable() -> Self {
        Self("stable".to_string())
    }

    pub fn canary() -> Self {
        Self("canary".to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ReleaseChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ModuleDigest(String);

impl ModuleDigest {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let hex = value.strip_prefix("sha256:").unwrap_or("");
        let valid = hex.len() == 64 && hex.chars().all(|ch| ch.is_ascii_hexdigit());

        if valid {
            Ok(Self(value.to_ascii_lowercase()))
        } else {
            Err(EhdbError::InvalidIdentifier(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ModuleDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SystemLibraryRevision(u32);

impl SystemLibraryRevision {
    pub fn new(value: u32) -> Result<Self> {
        if value == 0 {
            Err(EhdbError::InvalidState(
                "system library revision must be greater than zero".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }

    pub fn value(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WasmTarget {
    Wasm32UnknownUnknown,
    Wasm32WasiPreview1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemCapability {
    EventPublish,
    ObjectPut,
    ResultPut,
    EhdbCatalogRead,
    EhdbCatalogWrite,
    EhdbStreamPublish,
    EhdbRetrievalWrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmSystemLibrary {
    pub path: SystemLibraryPath,
    pub revision: SystemLibraryRevision,
    pub digest: ModuleDigest,
    pub entry: String,
    pub target: WasmTarget,
    pub object_path: ObjectPath,
    pub byte_len: u64,
    pub capabilities: Vec<SystemCapability>,
    pub created_by: TransactionId,
}

impl WasmSystemLibrary {
    pub fn plugin_ref(&self) -> NoetlWasmPluginRef {
        NoetlWasmPluginRef {
            path: self.path.clone(),
            version: self.revision,
            digest: self.digest.clone(),
            entry: self.entry.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoetlWasmPluginRef {
    pub path: SystemLibraryPath,
    pub version: SystemLibraryRevision,
    pub digest: ModuleDigest,
    pub entry: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishSystemLibrary {
    pub path: SystemLibraryPath,
    pub revision: SystemLibraryRevision,
    pub digest: ModuleDigest,
    pub entry: String,
    pub target: WasmTarget,
    pub object_path: ObjectPath,
    pub byte_len: u64,
    pub capabilities: Vec<SystemCapability>,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindSystemLibrary {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub environment: EnvironmentName,
    pub channel: ReleaseChannel,
    pub path: SystemLibraryPath,
    pub revision: SystemLibraryRevision,
    pub digest: ModuleDigest,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveSystemLibrary {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub environment: EnvironmentName,
    pub channel: ReleaseChannel,
    pub path: SystemLibraryPath,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LibraryKey {
    path: SystemLibraryPath,
    revision: SystemLibraryRevision,
    digest: ModuleDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BindingKey {
    tenant: TenantId,
    namespace: NamespaceName,
    environment: EnvironmentName,
    channel: ReleaseChannel,
    path: SystemLibraryPath,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemLibraryBinding {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub environment: EnvironmentName,
    pub channel: ReleaseChannel,
    pub path: SystemLibraryPath,
    pub revision: SystemLibraryRevision,
    pub digest: ModuleDigest,
    pub updated_by: TransactionId,
}

#[derive(Debug, Clone, Default)]
pub struct InMemorySystemLibraryCatalog {
    libraries: BTreeMap<LibraryKey, WasmSystemLibrary>,
    bindings: BTreeMap<BindingKey, SystemLibraryBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum SystemLibraryJournalEntry {
    Publish(PublishSystemLibrary),
    Bind(BindSystemLibrary),
}

#[derive(Debug)]
pub struct LocalJsonlSystemLibraryCatalog {
    path: PathBuf,
    inner: InMemorySystemLibraryCatalog,
}

impl LocalJsonlSystemLibraryCatalog {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut inner = InMemorySystemLibraryCatalog::default();

        if path.exists() {
            let file = File::open(&path).map_err(|err| EhdbError::Storage(err.to_string()))?;
            for (index, line) in BufReader::new(file).lines().enumerate() {
                let line = line.map_err(|err| EhdbError::Storage(err.to_string()))?;
                if line.trim().is_empty() {
                    continue;
                }
                let entry: SystemLibraryJournalEntry =
                    serde_json::from_str(&line).map_err(|err| {
                        EhdbError::Storage(format!(
                            "invalid system library journal record at line {}: {err}",
                            index + 1
                        ))
                    })?;
                apply_journal_entry(&mut inner, entry)?;
            }
        }

        Ok(Self { path, inner })
    }

    pub fn publish(&mut self, request: PublishSystemLibrary) -> Result<WasmSystemLibrary> {
        let mut next = self.inner.clone();
        let library = next.publish(request.clone())?;
        self.append_entry_to_disk(&SystemLibraryJournalEntry::Publish(request))?;
        self.inner = next;
        Ok(library)
    }

    pub fn bind(&mut self, request: BindSystemLibrary) -> Result<SystemLibraryBinding> {
        let mut next = self.inner.clone();
        let binding = next.bind(request.clone())?;
        self.append_entry_to_disk(&SystemLibraryJournalEntry::Bind(request))?;
        self.inner = next;
        Ok(binding)
    }

    pub fn resolve(&self, request: ResolveSystemLibrary) -> Result<WasmSystemLibrary> {
        self.inner.resolve(request)
    }

    pub fn binding_count(&self) -> usize {
        self.inner.binding_count()
    }

    pub fn library_count(&self) -> usize {
        self.inner.library_count()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn append_entry_to_disk(&self, entry: &SystemLibraryJournalEntry) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        serde_json::to_writer(&mut file, entry)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        file.write_all(b"\n")
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        file.sync_data()
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(())
    }
}

fn apply_journal_entry(
    inner: &mut InMemorySystemLibraryCatalog,
    entry: SystemLibraryJournalEntry,
) -> Result<()> {
    match entry {
        SystemLibraryJournalEntry::Publish(request) => inner.publish(request).map(|_| ()),
        SystemLibraryJournalEntry::Bind(request) => inner.bind(request).map(|_| ()),
    }
}

impl InMemorySystemLibraryCatalog {
    pub fn publish(&mut self, request: PublishSystemLibrary) -> Result<WasmSystemLibrary> {
        if request.entry.is_empty() {
            return Err(EhdbError::InvalidIdentifier(
                "system library entry export is required".to_string(),
            ));
        }
        if request.byte_len == 0 {
            return Err(EhdbError::InvalidState(
                "system library byte length must be greater than zero".to_string(),
            ));
        }
        if request.capabilities.is_empty() {
            return Err(EhdbError::InvalidState(
                "system library requires at least one host capability".to_string(),
            ));
        }

        let key = LibraryKey {
            path: request.path.clone(),
            revision: request.revision,
            digest: request.digest.clone(),
        };
        if self.libraries.contains_key(&key) {
            return Err(EhdbError::AlreadyExists(format!(
                "{}@{}#{}",
                key.path,
                key.revision.value(),
                key.digest
            )));
        }

        let library = WasmSystemLibrary {
            path: request.path,
            revision: request.revision,
            digest: request.digest,
            entry: request.entry,
            target: request.target,
            object_path: request.object_path,
            byte_len: request.byte_len,
            capabilities: request.capabilities,
            created_by: request.transaction_id,
        };
        self.libraries.insert(key, library.clone());
        Ok(library)
    }

    pub fn bind(&mut self, request: BindSystemLibrary) -> Result<SystemLibraryBinding> {
        self.library(&request.path, request.revision, &request.digest)?;
        let key = BindingKey {
            tenant: request.tenant.clone(),
            namespace: request.namespace.clone(),
            environment: request.environment.clone(),
            channel: request.channel.clone(),
            path: request.path.clone(),
        };
        let binding = SystemLibraryBinding {
            tenant: request.tenant,
            namespace: request.namespace,
            environment: request.environment,
            channel: request.channel,
            path: request.path,
            revision: request.revision,
            digest: request.digest,
            updated_by: request.transaction_id,
        };
        self.bindings.insert(key, binding.clone());
        Ok(binding)
    }

    pub fn resolve(&self, request: ResolveSystemLibrary) -> Result<WasmSystemLibrary> {
        let key = BindingKey {
            tenant: request.tenant,
            namespace: request.namespace,
            environment: request.environment,
            channel: request.channel,
            path: request.path,
        };
        let binding = self.bindings.get(&key).ok_or_else(|| {
            EhdbError::NotFound(format!(
                "{}.{}.{}.{}:{}",
                key.tenant, key.namespace, key.environment, key.channel, key.path
            ))
        })?;
        self.library(&binding.path, binding.revision, &binding.digest)
            .cloned()
    }

    pub fn binding_count(&self) -> usize {
        self.bindings.len()
    }

    pub fn library_count(&self) -> usize {
        self.libraries.len()
    }

    fn library(
        &self,
        path: &SystemLibraryPath,
        revision: SystemLibraryRevision,
        digest: &ModuleDigest,
    ) -> Result<&WasmSystemLibrary> {
        let key = LibraryKey {
            path: path.clone(),
            revision,
            digest: digest.clone(),
        };
        self.libraries
            .get(&key)
            .ok_or_else(|| EhdbError::NotFound(format!("{}@{}#{}", path, revision.value(), digest)))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn digest(suffix: char) -> ModuleDigest {
        ModuleDigest::new(format!("sha256:{}{}", "a".repeat(63), suffix)).unwrap()
    }

    fn tenant_namespace() -> (TenantId, NamespaceName) {
        (
            TenantId::new("tenant-a").unwrap(),
            NamespaceName::new("system").unwrap(),
        )
    }

    fn temp_log_path(test_name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ehdb-system-{test_name}-{}-{suffix}.jsonl",
            std::process::id()
        ))
    }

    fn publish_request(path: &str, revision: u32, suffix: char) -> PublishSystemLibrary {
        PublishSystemLibrary {
            path: SystemLibraryPath::new(path).unwrap(),
            revision: SystemLibraryRevision::new(revision).unwrap(),
            digest: digest(suffix),
            entry: "run".to_string(),
            target: WasmTarget::Wasm32UnknownUnknown,
            object_path: ObjectPath::new(format!("system-libraries/{path}/{revision}/module.wasm"))
                .unwrap(),
            byte_len: 128,
            capabilities: vec![SystemCapability::EhdbCatalogWrite],
            transaction_id: TransactionId::new(format!("txn-publish-{revision}-{suffix}")).unwrap(),
        }
    }

    #[test]
    fn publishes_and_resolves_environment_bound_system_library() {
        let (tenant, namespace) = tenant_namespace();
        let mut catalog = InMemorySystemLibraryCatalog::default();
        let library = catalog
            .publish(publish_request("system/catalog/bootstrap", 1, '1'))
            .unwrap();
        catalog
            .bind(BindSystemLibrary {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                environment: EnvironmentName::new("kind").unwrap(),
                channel: ReleaseChannel::stable(),
                path: library.path.clone(),
                revision: library.revision,
                digest: library.digest.clone(),
                transaction_id: TransactionId::new("txn-bind-1").unwrap(),
            })
            .unwrap();

        let resolved = catalog
            .resolve(ResolveSystemLibrary {
                tenant,
                namespace,
                environment: EnvironmentName::new("kind").unwrap(),
                channel: ReleaseChannel::stable(),
                path: SystemLibraryPath::new("system/catalog/bootstrap").unwrap(),
            })
            .unwrap();

        assert_eq!(resolved, library);
        assert_eq!(resolved.plugin_ref().version.value(), 1);
    }

    #[test]
    fn rebinds_channel_to_hot_replace_without_semver_churn() {
        let (tenant, namespace) = tenant_namespace();
        let mut catalog = InMemorySystemLibraryCatalog::default();
        let first = catalog
            .publish(publish_request("system/stream/materializer", 1, '1'))
            .unwrap();
        let replacement = catalog
            .publish(publish_request("system/stream/materializer", 2, '2'))
            .unwrap();

        for (library, txn) in [(&first, "txn-bind-1"), (&replacement, "txn-bind-2")] {
            catalog
                .bind(BindSystemLibrary {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    environment: EnvironmentName::new("gke-prod").unwrap(),
                    channel: ReleaseChannel::stable(),
                    path: library.path.clone(),
                    revision: library.revision,
                    digest: library.digest.clone(),
                    transaction_id: TransactionId::new(txn).unwrap(),
                })
                .unwrap();
        }

        let resolved = catalog
            .resolve(ResolveSystemLibrary {
                tenant,
                namespace,
                environment: EnvironmentName::new("gke-prod").unwrap(),
                channel: ReleaseChannel::stable(),
                path: SystemLibraryPath::new("system/stream/materializer").unwrap(),
            })
            .unwrap();

        assert_eq!(resolved.revision.value(), 2);
        assert_eq!(catalog.library_count(), 2);
        assert_eq!(catalog.binding_count(), 1);
    }

    #[test]
    fn resolves_different_implementations_per_environment() {
        let (tenant, namespace) = tenant_namespace();
        let mut catalog = InMemorySystemLibraryCatalog::default();
        let local = catalog
            .publish(publish_request("system/object/put", 1, '1'))
            .unwrap();
        let cloud = catalog
            .publish(PublishSystemLibrary {
                path: local.path.clone(),
                revision: SystemLibraryRevision::new(2).unwrap(),
                digest: digest('2'),
                entry: "run".to_string(),
                target: WasmTarget::Wasm32WasiPreview1,
                object_path: ObjectPath::new("system-libraries/system/object/put/2/module.wasm")
                    .unwrap(),
                byte_len: 256,
                capabilities: vec![SystemCapability::ObjectPut],
                transaction_id: TransactionId::new("txn-publish-cloud").unwrap(),
            })
            .unwrap();

        for (environment, library, txn) in [
            ("kind", &local, "txn-bind-kind"),
            ("gke-prod", &cloud, "txn-bind-gke"),
        ] {
            catalog
                .bind(BindSystemLibrary {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    environment: EnvironmentName::new(environment).unwrap(),
                    channel: ReleaseChannel::stable(),
                    path: library.path.clone(),
                    revision: library.revision,
                    digest: library.digest.clone(),
                    transaction_id: TransactionId::new(txn).unwrap(),
                })
                .unwrap();
        }

        let kind = catalog
            .resolve(ResolveSystemLibrary {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                environment: EnvironmentName::new("kind").unwrap(),
                channel: ReleaseChannel::stable(),
                path: local.path.clone(),
            })
            .unwrap();
        let gke = catalog
            .resolve(ResolveSystemLibrary {
                tenant,
                namespace,
                environment: EnvironmentName::new("gke-prod").unwrap(),
                channel: ReleaseChannel::stable(),
                path: local.path,
            })
            .unwrap();

        assert_eq!(
            kind.object_path.as_str(),
            "system-libraries/system/object/put/1/module.wasm"
        );
        assert_eq!(
            gke.object_path.as_str(),
            "system-libraries/system/object/put/2/module.wasm"
        );
    }

    #[test]
    fn rejects_invalid_paths_digests_and_unpublished_bindings() {
        assert!(SystemLibraryPath::new("../escape").is_err());
        assert!(SystemLibraryPath::new("/absolute").is_err());
        assert!(ModuleDigest::new("sha256:not-hex").is_err());
        assert!(SystemLibraryRevision::new(0).is_err());

        let (tenant, namespace) = tenant_namespace();
        let mut catalog = InMemorySystemLibraryCatalog::default();
        let error = catalog
            .bind(BindSystemLibrary {
                tenant,
                namespace,
                environment: EnvironmentName::new("kind").unwrap(),
                channel: ReleaseChannel::stable(),
                path: SystemLibraryPath::new("system/missing").unwrap(),
                revision: SystemLibraryRevision::new(1).unwrap(),
                digest: digest('1'),
                transaction_id: TransactionId::new("txn-bind-missing").unwrap(),
            })
            .unwrap_err();

        assert!(matches!(error, EhdbError::NotFound(_)));
    }

    #[test]
    fn local_jsonl_catalog_replays_binding_after_reopen() {
        let path = temp_log_path("restart");
        let (tenant, namespace) = tenant_namespace();
        let mut catalog = LocalJsonlSystemLibraryCatalog::open(&path).unwrap();
        let library = catalog
            .publish(publish_request("system/catalog/bootstrap", 1, '1'))
            .unwrap();
        catalog
            .bind(BindSystemLibrary {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                environment: EnvironmentName::new("kind").unwrap(),
                channel: ReleaseChannel::stable(),
                path: library.path.clone(),
                revision: library.revision,
                digest: library.digest.clone(),
                transaction_id: TransactionId::new("txn-bind-1").unwrap(),
            })
            .unwrap();
        drop(catalog);

        let reopened = LocalJsonlSystemLibraryCatalog::open(&path).unwrap();
        let resolved = reopened
            .resolve(ResolveSystemLibrary {
                tenant,
                namespace,
                environment: EnvironmentName::new("kind").unwrap(),
                channel: ReleaseChannel::stable(),
                path: SystemLibraryPath::new("system/catalog/bootstrap").unwrap(),
            })
            .unwrap();

        assert_eq!(resolved.revision.value(), 1);
        assert_eq!(reopened.path(), path.as_path());
        assert_eq!(reopened.library_count(), 1);
        assert_eq!(reopened.binding_count(), 1);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_catalog_replays_hot_replacement_after_reopen() {
        let path = temp_log_path("hot-replace");
        let (tenant, namespace) = tenant_namespace();
        let mut catalog = LocalJsonlSystemLibraryCatalog::open(&path).unwrap();
        let first = catalog
            .publish(publish_request("system/stream/materializer", 1, '1'))
            .unwrap();
        let replacement = catalog
            .publish(publish_request("system/stream/materializer", 2, '2'))
            .unwrap();

        for (library, txn) in [(&first, "txn-bind-1"), (&replacement, "txn-bind-2")] {
            catalog
                .bind(BindSystemLibrary {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    environment: EnvironmentName::new("gke-prod").unwrap(),
                    channel: ReleaseChannel::stable(),
                    path: library.path.clone(),
                    revision: library.revision,
                    digest: library.digest.clone(),
                    transaction_id: TransactionId::new(txn).unwrap(),
                })
                .unwrap();
        }
        drop(catalog);

        let reopened = LocalJsonlSystemLibraryCatalog::open(&path).unwrap();
        let resolved = reopened
            .resolve(ResolveSystemLibrary {
                tenant,
                namespace,
                environment: EnvironmentName::new("gke-prod").unwrap(),
                channel: ReleaseChannel::stable(),
                path: SystemLibraryPath::new("system/stream/materializer").unwrap(),
            })
            .unwrap();

        assert_eq!(resolved.revision.value(), 2);
        assert_eq!(reopened.library_count(), 2);
        assert_eq!(reopened.binding_count(), 1);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_catalog_rejects_corrupt_records_on_open() {
        let path = temp_log_path("corrupt");
        fs::write(&path, b"not-json\n").unwrap();

        let error = LocalJsonlSystemLibraryCatalog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));

        fs::remove_file(path).unwrap();
    }
}
