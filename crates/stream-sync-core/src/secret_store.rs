use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub const FAIL_CLOSED_SECRET_STORE_MESSAGE: &str =
    "secret store unavailable on this platform (fail-closed)";
pub const TWITCH_PERSONAL_ACCESS_KEY: &str = "twitch.personal.access_token";
pub const TWITCH_PERSONAL_REFRESH_KEY: &str = "twitch.personal.refresh_token";
pub const KICK_PERSONAL_ACCESS_KEY: &str = "kick.personal.access_token";
pub const KICK_PERSONAL_REFRESH_KEY: &str = "kick.personal.refresh_token";
pub const KICK_PERSONAL_FEED_TICKET_KEY: &str = "kick.personal.feed_ticket";
pub const STREAMELEMENTS_JWT_KEY: &str = "streamelements.jwt";
pub const TWITCH_DELEGATED_CONNECTION_KEY: &str = "twitch.delegated.connection_key";
pub const TWITCH_DELEGATED_ACCESS_TOKEN_KEY: &str = "twitch.delegated.access_token";
pub const TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY: &str = "twitch.delegated.kick_access_token";
pub const TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY: &str = "twitch.delegated.kick_refresh_token";

pub trait SecretStore: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn set(&self, key: &str, value: &[u8]) -> Result<()>;
    fn delete(&self, key: &str) -> Result<()>;
}

#[derive(Default)]
pub struct MemorySecretStore {
    inner: Mutex<HashMap<String, Vec<u8>>>,
}

impl SecretStore for MemorySecretStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| anyhow!("memory secret store lock poisoned"))?
            .get(key)
            .cloned())
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| anyhow!("memory secret store lock poisoned"))?
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| anyhow!("memory secret store lock poisoned"))?
            .remove(key);
        Ok(())
    }
}

#[derive(Default)]
pub struct FailClosedSecretStore;

impl SecretStore for FailClosedSecretStore {
    fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        Err(anyhow!(FAIL_CLOSED_SECRET_STORE_MESSAGE))
    }

    fn set(&self, _key: &str, _value: &[u8]) -> Result<()> {
        Err(anyhow!(FAIL_CLOSED_SECRET_STORE_MESSAGE))
    }

    fn delete(&self, _key: &str) -> Result<()> {
        Err(anyhow!(FAIL_CLOSED_SECRET_STORE_MESSAGE))
    }
}

pub fn memory_secret_store() -> Arc<dyn SecretStore> {
    Arc::new(MemorySecretStore::default())
}

/// Test adapter: secrets under `<root>/.streamsync-secret-store/`.
/// Not a production Linux store — production non-Windows is fail-closed.
pub fn fs_secret_store(root: impl AsRef<std::path::Path>) -> Arc<dyn SecretStore> {
    Arc::new(FsSecretStore {
        dir: root.as_ref().join(".streamsync-secret-store"),
    })
}

struct FsSecretStore {
    dir: std::path::PathBuf,
}

impl FsSecretStore {
    fn path_for(&self, key: &str) -> std::path::PathBuf {
        let safe: String = key
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        self.dir.join(safe)
    }
}

impl SecretStore for FsSecretStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.path_for(key);
        if !path.is_file() {
            return Ok(None);
        }
        Ok(Some(std::fs::read(&path).with_context(|| {
            format!("read secret {}", path.display())
        })?))
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("create {}", self.dir.display()))?;
        let path = self.path_for(key);
        std::fs::write(&path, value).with_context(|| format!("write secret {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.path_for(key);
        if path.is_file() {
            std::fs::remove_file(&path)
                .with_context(|| format!("delete secret {}", path.display()))?;
        }
        Ok(())
    }
}

pub fn runtime_secret_store() -> Arc<dyn SecretStore> {
    #[cfg(windows)]
    {
        Arc::new(WindowsCredentialManagerSecretStore::new())
    }
    #[cfg(not(windows))]
    {
        Arc::new(FailClosedSecretStore)
    }
}

#[cfg(windows)]
pub use windows_store::WindowsCredentialManagerSecretStore;

#[cfg(windows)]
mod windows_store {
    use super::SecretStore;
    use anyhow::{anyhow, Context, Result};
    use std::ffi::c_void;
    use windows_sys::Win32::Foundation::{GetLastError, ERROR_NOT_FOUND};
    use windows_sys::Win32::Security::Credentials::{
        CredDeleteW, CredFree, CredReadW, CredWriteW, CREDENTIALW, CRED_PERSIST_LOCAL_MACHINE,
        CRED_TYPE_GENERIC,
    };

    pub struct WindowsCredentialManagerSecretStore;

    impl WindowsCredentialManagerSecretStore {
        pub fn new() -> Self {
            Self
        }

        fn target_name(key: &str) -> Vec<u16> {
            format!("StreamSync/{key}\0").encode_utf16().collect()
        }
    }

    struct CredentialGuard(*mut c_void);

    impl Drop for CredentialGuard {
        fn drop(&mut self) {
            unsafe {
                CredFree(self.0);
            }
        }
    }

    impl SecretStore for WindowsCredentialManagerSecretStore {
        fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            let target = Self::target_name(key);
            unsafe {
                let mut credential: *mut CREDENTIALW = std::ptr::null_mut();
                if CredReadW(target.as_ptr(), CRED_TYPE_GENERIC, 0, &mut credential) == 0 {
                    let err = GetLastError();
                    if err == ERROR_NOT_FOUND {
                        return Ok(None);
                    }
                    return Err(anyhow!("windows credential manager read failed ({err})"));
                }
                let _guard = CredentialGuard(credential as *mut c_void);
                let cred = &*credential;
                let blob_size = usize::try_from(cred.CredentialBlobSize)
                    .context("credential blob size overflow")?;
                if blob_size == 0 {
                    return Ok(Some(Vec::new()));
                }
                let bytes = std::slice::from_raw_parts(cred.CredentialBlob as *const u8, blob_size)
                    .to_vec();
                Ok(Some(bytes))
            }
        }

        fn set(&self, key: &str, value: &[u8]) -> Result<()> {
            let target = Self::target_name(key);
            let mut blob = value.to_vec();
            let blob_size = u32::try_from(blob.len()).context("credential blob too large")?;
            let mut credential = CREDENTIALW {
                Flags: 0,
                Type: CRED_TYPE_GENERIC,
                TargetName: target.as_ptr() as *mut u16,
                Comment: std::ptr::null_mut(),
                LastWritten: unsafe { std::mem::zeroed() },
                CredentialBlobSize: blob_size,
                CredentialBlob: blob.as_mut_ptr(),
                Persist: CRED_PERSIST_LOCAL_MACHINE,
                AttributeCount: 0,
                Attributes: std::ptr::null_mut(),
                TargetAlias: std::ptr::null_mut(),
                UserName: std::ptr::null_mut(),
            };
            unsafe {
                if CredWriteW(&mut credential, 0) == 0 {
                    let err = GetLastError();
                    return Err(anyhow!("windows credential manager write failed ({err})"));
                }
            }
            Ok(())
        }

        fn delete(&self, key: &str) -> Result<()> {
            let target = Self::target_name(key);
            unsafe {
                if CredDeleteW(target.as_ptr(), CRED_TYPE_GENERIC, 0) == 0 {
                    let err = GetLastError();
                    if err == ERROR_NOT_FOUND {
                        return Ok(());
                    }
                    return Err(anyhow!("windows credential manager delete failed ({err})"));
                }
            }
            Ok(())
        }
    }
}
