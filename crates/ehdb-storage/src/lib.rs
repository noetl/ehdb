use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use ehdb_core::{EhdbError, NamespaceName, Result, SnapshotId, TableId, TenantId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPath(String);

impl ObjectPath {
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        let safe = !path.is_empty()
            && !path.starts_with('/')
            && !path.contains("..")
            && path
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '_' | '-' | '.'));

        if safe {
            Ok(Self(path))
        } else {
            Err(EhdbError::Storage(format!("unsafe object path: {path}")))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub fn table_snapshot_object_path(
    tenant: &TenantId,
    namespace: &NamespaceName,
    table: &TableId,
    snapshot: &SnapshotId,
    file_name: &str,
) -> Result<ObjectPath> {
    validate_file_name(file_name)?;
    ObjectPath::new(format!(
        "{tenant}/{namespace}/tables/{table}/snapshots/{snapshot}/{file_name}"
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectDigest(String);

impl ObjectDigest {
    pub fn sha256(bytes: &[u8]) -> Self {
        let digest = Sha256::digest(bytes);
        Self(format!("sha256:{digest:x}"))
    }

    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let valid = value
            .strip_prefix("sha256:")
            .is_some_and(|hex| hex.len() == 64 && hex.chars().all(|ch| ch.is_ascii_hexdigit()));

        if valid {
            Ok(Self(value))
        } else {
            Err(EhdbError::Storage(format!(
                "invalid object digest: {value}"
            )))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloudProvider {
    Local,
    Aws,
    Gcp,
    Azure,
    S3Compatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeoLocation {
    pub provider: CloudProvider,
    pub region: String,
    pub zone: Option<String>,
}

impl GeoLocation {
    pub fn new(
        provider: CloudProvider,
        region: impl Into<String>,
        zone: Option<impl Into<String>>,
    ) -> Result<Self> {
        let region = region.into();
        validate_placement_component("region", &region)?;
        let zone = zone.map(|zone| zone.into()).transpose_validated("zone")?;

        Ok(Self {
            provider,
            region,
            zone,
        })
    }

    pub fn local_dev() -> Self {
        Self {
            provider: CloudProvider::Local,
            region: "local-dev".to_string(),
            zone: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DataGravityShard(String);

impl DataGravityShard {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_placement_component("data gravity shard", &value)?;
        Ok(Self(value))
    }

    pub fn local_dev() -> Self {
        Self("local-dev".to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPlacement {
    pub geo: GeoLocation,
    pub data_gravity_shard: DataGravityShard,
}

impl ObjectPlacement {
    pub fn new(geo: GeoLocation, data_gravity_shard: DataGravityShard) -> Self {
        Self {
            geo,
            data_gravity_shard,
        }
    }

    pub fn local_dev() -> Self {
        Self {
            geo: GeoLocation::local_dev(),
            data_gravity_shard: DataGravityShard::local_dev(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRef {
    pub path: ObjectPath,
    pub len: u64,
    pub digest: ObjectDigest,
    pub placement: ObjectPlacement,
}

pub trait ImmutableObjectStore {
    fn put_if_absent(&self, path: ObjectPath, bytes: &[u8]) -> Result<ObjectRef>;
    fn get(&self, path: &ObjectPath) -> Result<Vec<u8>>;

    fn get_verified(&self, object: &ObjectRef) -> Result<Vec<u8>> {
        let bytes = self.get(&object.path)?;
        verify_object_bytes(object, &bytes)?;
        Ok(bytes)
    }
}

#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn resolve(&self, path: &ObjectPath) -> PathBuf {
        self.root.join(Path::new(path.as_str()))
    }
}

impl ImmutableObjectStore for LocalObjectStore {
    fn put_if_absent(&self, path: ObjectPath, bytes: &[u8]) -> Result<ObjectRef> {
        let target = self.resolve(&path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;

        file.write_all(bytes)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;

        Ok(ObjectRef {
            path,
            len: bytes.len() as u64,
            digest: ObjectDigest::sha256(bytes),
            placement: ObjectPlacement::local_dev(),
        })
    }

    fn get(&self, path: &ObjectPath) -> Result<Vec<u8>> {
        fs::read(self.resolve(path)).map_err(|err| EhdbError::Storage(err.to_string()))
    }
}

pub fn verify_object_bytes(object: &ObjectRef, bytes: &[u8]) -> Result<()> {
    if object.len != bytes.len() as u64 {
        return Err(EhdbError::Storage(format!(
            "object {} length mismatch: expected {}, got {}",
            object.path.as_str(),
            object.len,
            bytes.len()
        )));
    }

    let actual = ObjectDigest::sha256(bytes);
    if object.digest != actual {
        return Err(EhdbError::Storage(format!(
            "object {} digest mismatch: expected {}, got {}",
            object.path.as_str(),
            object.digest.as_str(),
            actual.as_str()
        )));
    }

    Ok(())
}

fn validate_file_name(file_name: &str) -> Result<()> {
    let valid = !file_name.is_empty()
        && !file_name.contains('/')
        && file_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'));

    if valid {
        Ok(())
    } else {
        Err(EhdbError::Storage(format!(
            "unsafe object file name: {file_name}"
        )))
    }
}

trait OptionalPlacementComponent {
    fn transpose_validated(self, label: &str) -> Result<Option<String>>;
}

impl OptionalPlacementComponent for Option<String> {
    fn transpose_validated(self, label: &str) -> Result<Option<String>> {
        self.map(|value| {
            validate_placement_component(label, &value)?;
            Ok(value)
        })
        .transpose()
    }
}

fn validate_placement_component(label: &str, value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'));

    if valid {
        Ok(())
    } else {
        Err(EhdbError::Storage(format!(
            "invalid {label} placement component: {value}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn ids() -> (TenantId, NamespaceName, TableId, SnapshotId) {
        (
            TenantId::new("tenant-a").unwrap(),
            NamespaceName::new("system").unwrap(),
            TableId::new("tenant-a_system_executions").unwrap(),
            SnapshotId::new("snapshot-0001").unwrap(),
        )
    }

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_root() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ehdb-storage-test-{}-{suffix}-{counter}",
            std::process::id()
        ))
    }

    #[test]
    fn writes_and_reads_immutable_object() {
        let root = temp_root();
        let store = LocalObjectStore::new(&root);
        let path = ObjectPath::new("tenant-a/system/executions/part-000.arrow").unwrap();

        let object = store
            .put_if_absent(path.clone(), b"arrow-ipc-placeholder")
            .unwrap();
        let bytes = store.get(&path).unwrap();

        assert_eq!(object.len, 21);
        assert_eq!(
            object.digest.as_str(),
            "sha256:f68b244fda3e7892b47146526f23ffd069dafb2ebba67ea8cb4f04c72da212dd"
        );
        assert_eq!(object.placement.geo.provider, CloudProvider::Local);
        assert_eq!(object.placement.geo.region, "local-dev");
        assert_eq!(object.placement.data_gravity_shard.as_str(), "local-dev");
        assert_eq!(bytes, b"arrow-ipc-placeholder");
        assert_eq!(
            store.get_verified(&object).unwrap(),
            b"arrow-ipc-placeholder"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detects_corrupt_object_bytes_on_verified_read() {
        let root = temp_root();
        let store = LocalObjectStore::new(&root);
        let path = ObjectPath::new("tenant-a/system/executions/part-000.arrow").unwrap();
        let object = store.put_if_absent(path.clone(), b"original").unwrap();

        fs::write(root.join(path.as_str()), b"corrupt").unwrap();
        let error = store.get_verified(&object).unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn validates_object_digest_format() {
        assert!(ObjectDigest::new(
            "sha256:6bb42ecc189fbd2b2f20cacb2caec839783454418e9db0e8b929ac1d26d67be2"
        )
        .is_ok());
        assert!(ObjectDigest::new("md5:6bb42ecc").is_err());
        assert!(ObjectDigest::new("sha256:not-hex").is_err());
    }

    #[test]
    fn validates_geo_location_and_data_gravity_shard() {
        let geo = GeoLocation::new(CloudProvider::Aws, "us-east-1", Some("use1-az1")).unwrap();
        let shard = DataGravityShard::new("tenant-a-system").unwrap();
        let placement = ObjectPlacement::new(geo, shard);

        assert_eq!(placement.geo.provider, CloudProvider::Aws);
        assert_eq!(placement.geo.region, "us-east-1");
        assert_eq!(placement.geo.zone.as_deref(), Some("use1-az1"));
        assert_eq!(placement.data_gravity_shard.as_str(), "tenant-a-system");
        assert!(GeoLocation::new(CloudProvider::Gcp, "us east1", None::<String>).is_err());
        assert!(DataGravityShard::new("tenant/a").is_err());
    }

    #[test]
    fn builds_table_snapshot_object_path() {
        let (tenant, namespace, table, snapshot) = ids();

        let path =
            table_snapshot_object_path(&tenant, &namespace, &table, &snapshot, "part-000.arrow")
                .unwrap();

        assert_eq!(
            path.as_str(),
            "tenant-a/system/tables/tenant-a_system_executions/snapshots/snapshot-0001/part-000.arrow"
        );
        assert!(table_snapshot_object_path(
            &tenant,
            &namespace,
            &table,
            &snapshot,
            "../part.arrow"
        )
        .is_err());
    }

    #[test]
    fn rejects_overwrite() {
        let root = temp_root();
        let store = LocalObjectStore::new(&root);
        let path = ObjectPath::new("tenant-a/system/executions/part-000.arrow").unwrap();

        store.put_if_absent(path.clone(), b"first").unwrap();
        let error = store.put_if_absent(path, b"second").unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_unsafe_object_paths() {
        assert!(ObjectPath::new("../secret").is_err());
        assert!(ObjectPath::new("/absolute/path").is_err());
        assert!(ObjectPath::new("tenant a/object").is_err());
    }
}
