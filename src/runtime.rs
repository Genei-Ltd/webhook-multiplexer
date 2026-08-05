use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    domain::InstanceName,
    protocol::{CONTROL_PROTOCOL_VERSION, ControlDescriptor},
};

use platform::{
    default_state_root, private_open_options, replace_descriptor,
    set_private_directory_permissions, verify_private_directory,
};

const STATE_DIRECTORY_NAME: &str = "webhook-multiplexer";

#[derive(Clone, Debug)]
pub struct RuntimePaths {
    instance_directory: PathBuf,
    descriptor: PathBuf,
    lock: PathBuf,
    manage_root: bool,
}

impl RuntimePaths {
    #[must_use]
    pub fn new(state_directory: Option<&Path>, instance: &InstanceName) -> Self {
        let (root, manage_root) = match state_directory {
            Some(path) => (path.to_path_buf(), false),
            None => (default_state_root(), true),
        };
        let instance_directory = root.join(instance.as_str());
        Self {
            descriptor: instance_directory.join("control.json"),
            lock: instance_directory.join("instance.lock"),
            instance_directory,
            manage_root,
        }
    }

    pub fn prepare(&self) -> Result<(), RuntimeError> {
        fs::create_dir_all(&self.instance_directory).map_err(RuntimeError::CreateDirectory)?;
        if self.manage_root
            && let Some(root) = self.instance_directory.parent()
        {
            verify_private_directory(root)?;
            set_private_directory_permissions(root)?;
        }
        verify_private_directory(&self.instance_directory)?;
        set_private_directory_permissions(&self.instance_directory)?;
        Ok(())
    }

    pub fn acquire_instance_lock(&self) -> Result<InstanceLock, RuntimeError> {
        self.prepare()?;
        let file = private_open_options()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.lock)
            .map_err(RuntimeError::OpenLock)?;
        FileExt::try_lock_exclusive(&file).map_err(|error| {
            // Contention is EWOULDBLOCK on Unix but ERROR_LOCK_VIOLATION on
            // Windows, which does not map to `ErrorKind::WouldBlock`.
            let contended = error.kind() == std::io::ErrorKind::WouldBlock
                || error.raw_os_error() == fs2::lock_contended_error().raw_os_error();
            if contended {
                RuntimeError::AlreadyRunning
            } else {
                RuntimeError::Lock(error)
            }
        })?;
        Ok(InstanceLock { _file: file })
    }

    pub fn write_descriptor(&self, descriptor: &ControlDescriptor) -> Result<(), RuntimeError> {
        self.prepare()?;
        let temporary = self
            .instance_directory
            .join(format!(".control.{}.tmp", Uuid::new_v4()));
        let bytes = serde_json::to_vec_pretty(descriptor).map_err(RuntimeError::Serialize)?;
        let mut file = private_open_options()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(RuntimeError::WriteDescriptor)?;
        let write_result = file
            .write_all(&bytes)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all());
        drop(file);
        if let Err(error) = write_result {
            let _cleanup_result = fs::remove_file(&temporary);
            return Err(RuntimeError::WriteDescriptor(error));
        }
        if let Err(error) = replace_descriptor(&temporary, &self.descriptor) {
            let _cleanup_result = fs::remove_file(&temporary);
            return Err(RuntimeError::ReplaceDescriptor(error));
        }
        Ok(())
    }

    pub fn read_descriptor(&self) -> Result<ControlDescriptor, RuntimeError> {
        let bytes = fs::read(&self.descriptor).map_err(RuntimeError::ReadDescriptor)?;
        let descriptor: ControlDescriptor =
            serde_json::from_slice(&bytes).map_err(RuntimeError::ParseDescriptor)?;
        if descriptor.protocol_version != CONTROL_PROTOCOL_VERSION {
            return Err(RuntimeError::ProtocolVersion {
                expected: CONTROL_PROTOCOL_VERSION,
                actual: descriptor.protocol_version,
            });
        }
        Ok(descriptor)
    }

    pub fn remove_descriptor(&self) -> Result<(), RuntimeError> {
        match fs::remove_file(&self.descriptor) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RuntimeError::RemoveDescriptor(error)),
        }
    }

    #[must_use]
    pub fn descriptor_path(&self) -> &Path {
        &self.descriptor
    }
}

#[derive(Debug)]
pub struct InstanceLock {
    _file: File,
}

#[cfg(unix)]
mod platform {
    use std::{
        fs::{self, OpenOptions},
        os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        path::{Path, PathBuf},
    };

    use super::{RuntimeError, STATE_DIRECTORY_NAME};

    /// The default root is scoped per user because the system temporary
    /// directory is world-writable, where a shared name would let another
    /// local user pre-create the root and control the parent of every
    /// instance directory.
    pub fn default_state_root() -> PathBuf {
        let uid = rustix::process::getuid().as_raw();
        std::env::temp_dir().join(format!("{STATE_DIRECTORY_NAME}-{uid}"))
    }

    pub fn verify_private_directory(path: &Path) -> Result<(), RuntimeError> {
        let metadata = fs::symlink_metadata(path).map_err(RuntimeError::InspectDirectory)?;
        let owned_by_current_user = metadata.uid() == rustix::process::getuid().as_raw();
        if metadata.is_dir() && owned_by_current_user {
            Ok(())
        } else {
            Err(RuntimeError::UntrustedDirectory(path.to_path_buf()))
        }
    }

    pub fn private_open_options() -> OpenOptions {
        let mut options = OpenOptions::new();
        options.mode(0o600);
        options
    }

    pub fn set_private_directory_permissions(path: &Path) -> Result<(), RuntimeError> {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(RuntimeError::SetPermissions)
    }

    pub fn replace_descriptor(temporary: &Path, descriptor: &Path) -> std::io::Result<()> {
        fs::rename(temporary, descriptor)
    }
}

#[cfg(not(unix))]
mod platform {
    use std::{
        fs::{self, OpenOptions},
        path::{Path, PathBuf},
    };

    use super::{RuntimeError, STATE_DIRECTORY_NAME};

    pub fn default_state_root() -> PathBuf {
        std::env::temp_dir().join(STATE_DIRECTORY_NAME)
    }

    pub fn verify_private_directory(_path: &Path) -> Result<(), RuntimeError> {
        Ok(())
    }

    pub fn private_open_options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn set_private_directory_permissions(_path: &Path) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Renaming over an existing file fails on Windows, so the previous
    /// descriptor is removed first.
    pub fn replace_descriptor(temporary: &Path, descriptor: &Path) -> std::io::Result<()> {
        match fs::remove_file(descriptor) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::rename(temporary, descriptor)
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to create the runtime state directory: {0}")]
    CreateDirectory(std::io::Error),
    #[error("failed to inspect the runtime state directory: {0}")]
    InspectDirectory(std::io::Error),
    #[error("the state directory at {0} must be a directory owned by the current user")]
    UntrustedDirectory(PathBuf),
    #[error("failed to make the runtime state directory private: {0}")]
    SetPermissions(std::io::Error),
    #[error("failed to open the instance lock: {0}")]
    OpenLock(std::io::Error),
    #[error("another server is already running for this instance")]
    AlreadyRunning,
    #[error("failed to lock the instance state: {0}")]
    Lock(std::io::Error),
    #[error("failed to serialize the control descriptor: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to write the control descriptor: {0}")]
    WriteDescriptor(std::io::Error),
    #[error("failed to replace the control descriptor: {0}")]
    ReplaceDescriptor(std::io::Error),
    #[error("failed to read the control descriptor: {0}")]
    ReadDescriptor(std::io::Error),
    #[error("failed to parse the control descriptor: {0}")]
    ParseDescriptor(serde_json::Error),
    #[error("control protocol version {actual} is incompatible; expected {expected}")]
    ProtocolVersion { expected: u16, actual: u16 },
    #[error("failed to remove the control descriptor: {0}")]
    RemoveDescriptor(std::io::Error),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::str::FromStr;

    use tempfile::TempDir;

    use super::{RuntimeError, RuntimePaths};
    use crate::{
        domain::InstanceName,
        protocol::{CONTROL_PROTOCOL_VERSION, ControlDescriptor},
    };

    #[test]
    fn descriptor_round_trips_without_exposing_other_instances() {
        let temporary = TempDir::new().expect("temporary directory");
        let first = RuntimePaths::new(
            Some(temporary.path()),
            &InstanceName::from_str("first").expect("valid instance"),
        );
        let second = RuntimePaths::new(
            Some(temporary.path()),
            &InstanceName::from_str("second").expect("valid instance"),
        );
        let descriptor = ControlDescriptor {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            address: "127.0.0.1:40123".to_owned(),
            token: "secret-token".to_owned(),
            process_id: 123,
        };

        first
            .write_descriptor(&descriptor)
            .expect("write descriptor");

        assert_eq!(
            first.read_descriptor().expect("read descriptor").address,
            descriptor.address
        );
        assert!(second.read_descriptor().is_err());
        assert!(!format!("{descriptor:?}").contains("secret-token"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let descriptor_mode = std::fs::metadata(first.descriptor_path())
                .expect("descriptor metadata")
                .permissions()
                .mode()
                & 0o777;
            let directory_mode = std::fs::metadata(
                first
                    .descriptor_path()
                    .parent()
                    .expect("instance directory"),
            )
            .expect("instance directory metadata")
            .permissions()
            .mode()
                & 0o777;
            assert_eq!(descriptor_mode, 0o600);
            assert_eq!(directory_mode, 0o700);
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_instance_directories_are_rejected() {
        let temporary = TempDir::new().expect("temporary directory");
        let elsewhere = TempDir::new().expect("symlink target directory");
        let paths = RuntimePaths::new(
            Some(temporary.path()),
            &InstanceName::from_str("linked").expect("valid instance"),
        );
        std::os::unix::fs::symlink(elsewhere.path(), temporary.path().join("linked"))
            .expect("create symlink");

        assert!(matches!(
            paths.prepare(),
            Err(RuntimeError::UntrustedDirectory(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn default_state_root_is_scoped_per_user() {
        let paths = RuntimePaths::new(
            None,
            &InstanceName::from_str("scoped").expect("valid instance"),
        );
        let uid = rustix::process::getuid().as_raw();

        assert!(
            paths
                .descriptor_path()
                .to_string_lossy()
                .contains(&format!("webhook-multiplexer-{uid}"))
        );
    }

    #[test]
    fn one_server_owns_each_instance() {
        let temporary = TempDir::new().expect("temporary directory");
        let paths = RuntimePaths::new(
            Some(temporary.path()),
            &InstanceName::from_str("shared").expect("valid instance"),
        );
        let _first_lock = paths.acquire_instance_lock().expect("first server lock");

        assert!(matches!(
            paths.acquire_instance_lock(),
            Err(RuntimeError::AlreadyRunning)
        ));
    }
}
