use anda_core::{BoxError, RequestMeta};
use std::{
    ffi::OsString,
    fs::{Metadata, Permissions},
    path::{Component, Path, PathBuf},
};
use tokio::io::AsyncWriteExt;

mod edit;
mod read;
mod search;
mod write;

pub use edit::*;
pub use read::*;
pub use search::*;
pub use write::*;

pub(crate) const MAX_FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024;

pub(crate) const UTF8_ENCODING: &str = "utf8";
pub(crate) const BASE64_ENCODING: &str = "base64";

#[derive(Debug, Clone)]
pub(crate) struct ResolvedFilePath {
    pub(crate) workspace: PathBuf,
    pub(crate) path: PathBuf,
}

pub(crate) fn normalize_workspaces<I>(workspaces: I) -> Vec<PathBuf>
where
    I: IntoIterator<Item = PathBuf>,
{
    let mut normalized = Vec::new();
    for workspace in workspaces {
        push_workspace(&mut normalized, workspace);
    }

    normalized
}

pub(crate) fn tool_workspaces(meta: &RequestMeta, defaults: &[PathBuf]) -> Vec<PathBuf> {
    let mut workspaces = Vec::new();

    if let Some(workspace) = meta.get_extra_as::<PathBuf>("workspace") {
        push_workspace(&mut workspaces, workspace);
    } else if let Some(extra_workspaces) = meta.get_extra_as::<Vec<PathBuf>>("workspace") {
        for workspace in extra_workspaces {
            push_workspace(&mut workspaces, workspace);
        }
    }

    if let Some(workspace) = meta.get_extra_as::<PathBuf>("workspaces") {
        push_workspace(&mut workspaces, workspace);
    } else if let Some(extra_workspaces) = meta.get_extra_as::<Vec<PathBuf>>("workspaces") {
        for workspace in extra_workspaces {
            push_workspace(&mut workspaces, workspace);
        }
    }

    for workspace in defaults {
        push_workspace(&mut workspaces, workspace.clone());
    }

    workspaces
}

pub(crate) fn format_workspaces(workspaces: &[PathBuf]) -> String {
    if workspaces.is_empty() {
        return "<none>".to_string();
    }

    workspaces
        .iter()
        .map(|workspace| workspace.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn push_workspace(workspaces: &mut Vec<PathBuf>, workspace: PathBuf) {
    if workspace.as_os_str().is_empty() {
        return;
    }

    if !workspaces.iter().any(|existing| existing == &workspace) {
        workspaces.push(workspace);
    }
}

pub(crate) async fn resolve_read_path_in_workspaces(
    workspaces: &[PathBuf],
    user_path: &str,
) -> Result<ResolvedFilePath, BoxError> {
    let mut errors = Vec::new();

    for workspace in workspaces {
        match resolve_read_path(workspace, user_path).await {
            Ok(path) => {
                return Ok(ResolvedFilePath {
                    workspace: workspace.clone(),
                    path,
                });
            }
            Err(err) => errors.push(format!("{}: {err}", workspace.display())),
        }
    }

    Err(workspace_access_error(
        "Path",
        "requested_path",
        user_path,
        workspaces,
        errors,
    ))
}

pub(crate) async fn resolve_write_path_in_workspaces(
    workspaces: &[PathBuf],
    user_path: &str,
) -> Result<ResolvedFilePath, BoxError> {
    let requested_path = Path::new(user_path);

    if requested_path.is_relative() {
        for workspace in workspaces {
            let candidate_path = workspace.join(requested_path);
            match tokio::fs::symlink_metadata(&candidate_path).await {
                Ok(_) => {
                    let path = resolve_write_path(workspace, user_path).await?;
                    return Ok(ResolvedFilePath {
                        workspace: workspace.clone(),
                        path,
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(format!(
                        "Failed to inspect file path (workspace: {}, requested_path: {}, candidate_path: {}): {err}",
                        workspace.display(),
                        user_path,
                        candidate_path.display()
                    )
                    .into());
                }
            }
        }
    }

    let mut errors = Vec::new();
    for workspace in workspaces {
        match resolve_write_path(workspace, user_path).await {
            Ok(path) => {
                return Ok(ResolvedFilePath {
                    workspace: workspace.clone(),
                    path,
                });
            }
            Err(err) => errors.push(format!("{}: {err}", workspace.display())),
        }
    }

    Err(workspace_access_error(
        "Path",
        "requested_path",
        user_path,
        workspaces,
        errors,
    ))
}

pub(crate) fn workspace_access_error(
    subject: &str,
    request_label: &str,
    requested_value: &str,
    workspaces: &[PathBuf],
    errors: Vec<String>,
) -> BoxError {
    let details = if errors.is_empty() {
        String::new()
    } else {
        format!("; errors: {}", errors.join("; "))
    };

    format!(
        "{subject} is not accessible from any configured workspace ({request_label}: {}, workspaces: [{}]){}",
        requested_value,
        format_workspaces(workspaces),
        details
    )
    .into()
}

/// Resolves an existing read target reachable from the workspace namespace.
pub async fn resolve_read_path(workspace: &Path, user_path: &str) -> Result<PathBuf, BoxError> {
    let resolved_workspace = resolve_workspace_path(workspace).await?;
    let requested_path = Path::new(user_path);
    let path = workspace.join(requested_path);

    if !path_contains_parent_reference(requested_path) {
        ensure_path_in_workspace_namespace(workspace, &resolved_workspace, &path)?;

        return tokio::fs::canonicalize(&path)
            .await
            .map_err(|err| {
                format!(
                    "Failed to resolve file path (workspace: {}, requested_path: {}, candidate_path: {}): {err}",
                    workspace.display(),
                    requested_path.display(),
                    path.display()
                )
                .into()
            });
    }

    let resolved_path = tokio::fs::canonicalize(&path)
        .await
        .map_err(|err| {
            format!(
                "Failed to resolve file path (workspace: {}, requested_path: {}, candidate_path: {}): {err}",
                workspace.display(),
                requested_path.display(),
                path.display()
            )
        })?;

    ensure_path_in_workspace(&resolved_workspace, &resolved_path)?;

    Ok(resolved_path)
}

/// Resolves a write target inside the workspace, even when the destination does not yet exist.
pub async fn resolve_write_path(workspace: &Path, user_path: &str) -> Result<PathBuf, BoxError> {
    let resolved_workspace = resolve_workspace_path(workspace).await?;
    let path = workspace.join(user_path);

    match tokio::fs::symlink_metadata(&path).await {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(format!(
                    "Writing to symbolic links is not allowed (workspace: {}, path: {})",
                    workspace.display(),
                    path.display()
                )
                .into());
            }

            let resolved_path = tokio::fs::canonicalize(&path)
                .await
                .map_err(|err| {
                    format!(
                        "Failed to resolve file path (workspace: {}, requested_path: {}, candidate_path: {}): {err}",
                        workspace.display(),
                        user_path,
                        path.display()
                    )
                })?;
            ensure_path_in_workspace(&resolved_workspace, &resolved_path)?;

            Ok(resolved_path)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let (existing_ancestor, missing_components) = nearest_existing_ancestor(&path).await?;
            let resolved_ancestor = tokio::fs::canonicalize(&existing_ancestor)
                .await
                .map_err(|err| {
                    format!(
                        "Failed to resolve file path ancestor (workspace: {}, requested_path: {}, ancestor_path: {}): {err}",
                        workspace.display(),
                        user_path,
                        existing_ancestor.display()
                    )
                })?;
            ensure_path_in_workspace(&resolved_workspace, &resolved_ancestor)?;

            Ok(missing_components
                .into_iter()
                .rev()
                .fold(resolved_ancestor, |acc, component| acc.join(component)))
        }
        Err(err) => Err(format!(
            "Failed to inspect file path (workspace: {}, path: {}): {err}",
            workspace.display(),
            path.display()
        )
        .into()),
    }
}

pub(crate) async fn resolve_workspace_path(workspace: &Path) -> Result<PathBuf, BoxError> {
    tokio::fs::canonicalize(workspace).await.map_err(|err| {
        format!(
            "Failed to resolve workspace path (workspace: {}): {err}",
            workspace.display()
        )
        .into()
    })
}

pub(crate) fn ensure_path_in_workspace(
    resolved_workspace: &Path,
    resolved_path: &Path,
) -> Result<(), BoxError> {
    if !resolved_path.starts_with(resolved_workspace) {
        return Err(format!(
            "Access to paths outside the workspace is not allowed (resolved_workspace: {}, resolved_path: {})",
            resolved_workspace.display(),
            resolved_path.display()
        )
        .into());
    }

    Ok(())
}

/// Returns true when the requested path contains a parent directory traversal.
pub(crate) fn path_contains_parent_reference(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir))
}

/// Ensures the requested path stays within the workspace namespace before following symlinks.
pub(crate) fn ensure_path_in_workspace_namespace(
    workspace: &Path,
    resolved_workspace: &Path,
    requested_path: &Path,
) -> Result<(), BoxError> {
    if requested_path.starts_with(workspace) || requested_path.starts_with(resolved_workspace) {
        return Ok(());
    }

    Err(format!(
        "Access to paths outside the workspace is not allowed (workspace: {}, resolved_workspace: {}, requested_path: {})",
        workspace.display(),
        resolved_workspace.display(),
        requested_path.display()
    )
    .into())
}

/// Returns the default encoding used for file writes.
pub(crate) fn default_write_encoding() -> String {
    UTF8_ENCODING.to_string()
}

/// Returns true when a file has multiple hard links.
///
/// Multiple links can allow path-based workspace guards to be bypassed by
/// linking a workspace path to external sensitive content.
pub(crate) fn has_multiple_hard_links(metadata: &Metadata) -> bool {
    link_count(metadata) > 1
}

pub(crate) fn ensure_regular_file(
    metadata: &Metadata,
    path: &Path,
    hard_link_error: &str,
) -> Result<(), BoxError> {
    if has_multiple_hard_links(metadata) {
        return Err(format!("{} (path: {})", hard_link_error, path.display()).into());
    }

    if !metadata.is_file() {
        return Err(format!(
            "Path does not point to a regular file (path: {})",
            path.display()
        )
        .into());
    }

    Ok(())
}

pub(crate) fn ensure_file_size_within_limit(
    metadata: &Metadata,
    path: &Path,
    max_size_bytes: u64,
) -> Result<(), BoxError> {
    if metadata.len() > max_size_bytes {
        return Err(format!(
            "File size {} exceeds maximum allowed size of {} bytes (path: {})",
            metadata.len(),
            max_size_bytes,
            path.display()
        )
        .into());
    }

    Ok(())
}

#[cfg(unix)]
fn link_count(metadata: &Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink()
}

#[cfg(windows)]
fn link_count(_metadata: &Metadata) -> u64 {
    // Rust stable does not currently expose a portable, stable Windows hard-link
    // count API on `std::fs::Metadata`. Returning 1 avoids false positive blocks
    // and keeps Windows builds stable until a supported API is available.
    1
}

#[cfg(not(any(unix, windows)))]
fn link_count(_metadata: &Metadata) -> u64 {
    1
}

/// Atomically writes data to a file by first writing to a temporary file and then renaming it into place.
pub async fn atomic_write_file(
    target_path: &Path,
    data: &[u8],
    existing_permissions: Option<&Permissions>,
) -> Result<(), BoxError> {
    let temp_path =
        write_temp_file_for_atomic_replace(target_path, data, existing_permissions).await?;

    if let Err(err) = commit_atomic_replace(&temp_path, target_path).await {
        let _ = tokio::fs::remove_file(&temp_path).await;
        return Err(err);
    }

    Ok(())
}

pub(crate) async fn write_temp_file_for_atomic_replace(
    target_path: &Path,
    data: &[u8],
    existing_permissions: Option<&Permissions>,
) -> Result<PathBuf, BoxError> {
    for _ in 0..16 {
        let temp_path = atomic_temp_path(target_path)?;
        let mut file = match tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .await
        {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(format!(
                    "Failed to create temporary file (target_path: {}, temp_path: {}): {err}",
                    target_path.display(),
                    temp_path.display()
                )
                .into());
            }
        };

        let write_result = async {
            file.write_all(data)
                .await
                .map_err(|err| {
                    format!(
                        "Failed to write temporary file (target_path: {}, temp_path: {}): {err}",
                        target_path.display(),
                        temp_path.display()
                    )
                })?;

            if let Some(permissions) = existing_permissions {
                tokio::fs::set_permissions(&temp_path, permissions.clone())
                    .await
                    .map_err(|err| {
                        format!(
                            "Failed to apply file permissions (target_path: {}, temp_path: {}): {err}",
                            target_path.display(),
                            temp_path.display()
                        )
                    })?;
            }

            file.sync_all()
                .await
                .map_err(|err| {
                    format!(
                        "Failed to sync temporary file (target_path: {}, temp_path: {}): {err}",
                        target_path.display(),
                        temp_path.display()
                    )
                })?;

            Ok::<(), BoxError>(())
        }
        .await;
        drop(file);

        if let Err(err) = write_result {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(err);
        }

        return Ok(temp_path);
    }

    Err(format!(
        "Failed to allocate unique temporary file for atomic write (target_path: {})",
        target_path.display()
    )
    .into())
}

pub(crate) async fn commit_atomic_replace(
    temp_path: &Path,
    target_path: &Path,
) -> Result<(), BoxError> {
    tokio::fs::rename(temp_path, target_path)
        .await
        .map_err(|err| {
            format!(
                "Failed to atomically replace file (temp_path: {}, target_path: {}): {err}",
                temp_path.display(),
                target_path.display()
            )
            .into()
        })
}

fn atomic_temp_path(target_path: &Path) -> Result<PathBuf, BoxError> {
    let parent = target_path.parent().ok_or_else(|| {
        format!(
            "Failed to determine parent directory for write target (target_path: {})",
            target_path.display()
        )
    })?;
    let file_name = target_path.file_name().ok_or_else(|| {
        format!(
            "Failed to determine file name for write target (target_path: {})",
            target_path.display()
        )
    })?;

    let mut temp_name = OsString::from(".");
    temp_name.push(file_name);
    temp_name.push(format!(".anda-tmp-{:016x}", rand::random::<u64>()));

    Ok(parent.join(temp_name))
}

/// Finds the nearest existing path component and returns the missing tail components.
pub(crate) async fn nearest_existing_ancestor(
    path: &Path,
) -> Result<(PathBuf, Vec<OsString>), BoxError> {
    let mut current = path.to_path_buf();
    let mut missing_components = Vec::new();

    loop {
        match tokio::fs::symlink_metadata(&current).await {
            Ok(_) => return Ok((current, missing_components)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let file_name = current.file_name().ok_or_else(|| {
                    format!(
                        "Access to paths outside the workspace is not allowed while resolving ancestor (requested_path: {}, current_path: {})",
                        path.display(),
                        current.display()
                    )
                })?;
                missing_components.push(file_name.to_os_string());
                current = current
                    .parent()
                    .ok_or_else(|| {
                        format!(
                            "Access to paths outside the workspace is not allowed while resolving ancestor (requested_path: {}, current_path: {})",
                            path.display(),
                            current.display()
                        )
                    })?
                    .to_path_buf();
            }
            Err(err) => {
                return Err(format!(
                    "Failed to inspect file path while resolving ancestor (requested_path: {}, current_path: {}): {err}",
                    path.display(),
                    current.display()
                )
                .into())
            }
        }
    }
}

pub(crate) fn normalize_relative_path(path: &Path) -> String {
    let value = path
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    if value.is_empty() {
        ".".to_string()
    } else {
        value
    }
}
