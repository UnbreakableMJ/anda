use anda_core::{BoxError, StateFeatures, ToolOutput};
use async_trait::async_trait;
use ic_auth_types::Xid;
use serde_json::json;
use std::{
    borrow::Cow,
    collections::HashMap,
    path::PathBuf,
    process::{ExitStatus, Output, Stdio},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::Mutex as TokioMutex,
};

use super::{ExecArgs, ExecOutput, Executor, ShellToolHook};
use crate::{
    context::BaseCtx,
    hook::{DynToolJsonHook, ToolBackgroundHook, ToolHook},
};

#[cfg(not(test))]
const BACKGROUND_PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);
#[cfg(test)]
const BACKGROUND_PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
const OUTPUT_READ_CHUNK_BYTES: usize = 8192;

type OutputBuffer = std::sync::Arc<TokioMutex<Vec<u8>>>;

/// Native runtime — full access, runs on Mac/Linux/Docker/Raspberry Pi
pub struct NativeRuntime {
    workspace: PathBuf,
    temp_dir: PathBuf,
    insecure: bool,
}

impl NativeRuntime {
    pub fn build_shell_command(command: &str) -> std::process::Command {
        #[cfg(not(target_os = "windows"))]
        {
            let mut process = std::process::Command::new("sh");
            process.arg("-c").arg(command);
            process
        }

        #[cfg(target_os = "windows")]
        {
            let mut process = std::process::Command::new("cmd.exe");
            process.arg("/C").arg(command);
            process
        }
    }

    pub fn new(workspace: PathBuf) -> Self {
        Self {
            workspace,
            temp_dir: std::env::temp_dir(),
            insecure: false,
        }
    }

    pub fn temp_dir(self, temp_dir: PathBuf) -> Self {
        Self { temp_dir, ..self }
    }

    pub fn insecure(self) -> Self {
        Self {
            insecure: true,
            ..self
        }
    }

    pub async fn execute_command(
        &self,
        ctx: BaseCtx,
        tool_name: &str,
        command: std::process::Command,
        envs: HashMap<String, String>,
        args: Option<ExecArgs>,
    ) -> Result<ExecOutput, BoxError> {
        let args = args.unwrap_or_default();
        let hook = ctx.get_state::<ShellToolHook>();
        let workspace = ctx
            .meta()
            .get_extra_as::<String>("workspace")
            .map(PathBuf::from)
            .map(Cow::Owned)
            .unwrap_or_else(|| Cow::Borrowed(&self.workspace));
        let workspace_str = workspace.to_string_lossy().to_string();

        let mut cmd = Command::from(command);
        if !self.insecure {
            cmd.env_clear();
        }

        cmd.envs(envs);
        cmd.current_dir(workspace.as_ref());
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd.kill_on_drop(true);

        let child = match cmd.spawn() {
            Ok(child) => child,
            Err(err) => {
                return Ok(ExecOutput {
                    workspace: Some(workspace_str),
                    stderr: Some(format!("Failed to spawn process: {err}")),
                    ..Default::default()
                });
            }
        };
        let pid = child.id();
        if !args.background {
            match child.wait_with_output().await {
                Ok(output) => {
                    let mut exec_output =
                        ExecOutput::from_output(pid, Some(output), &self.temp_dir).await;
                    exec_output.workspace = Some(workspace_str);
                    return Ok(exec_output);
                }
                Err(err) => {
                    let exec_output = ExecOutput {
                        workspace: Some(workspace_str),
                        process_id: pid,
                        stderr: Some(format!("Failed to execute background process: {err}")),
                        ..Default::default()
                    };
                    return Ok(exec_output);
                }
            }
        }

        let task_id = format!("{}:{}", tool_name, Xid::new());
        let exec_output = ExecOutput::from_output(
            pid,
            Some(Output {
                status: ExitStatus::default(),
                stdout: format!("Background process started with task ID {task_id}").into_bytes(),
                stderr: Vec::new(),
            }),
            &self.temp_dir,
        )
        .await;
        let json_hook = ctx.get_state::<DynToolJsonHook>();
        if let Some(hook) = &json_hook {
            hook.on_background_start(&ctx, &task_id, json!(&args)).await;
        } else if let Some(hook) = &hook {
            hook.on_background_start(&ctx, &task_id, &args).await;
        }

        {
            let temp_dir = self.temp_dir.clone();
            tokio::spawn(async move {
                let mut child = child;
                let stdout = std::sync::Arc::new(TokioMutex::new(Vec::new()));
                let stderr = std::sync::Arc::new(TokioMutex::new(Vec::new()));
                let stdout_reader = spawn_output_reader(child.stdout.take(), stdout.clone());
                let stderr_reader = spawn_output_reader(child.stderr.take(), stderr.clone());
                let mut stdout_progress = ProgressStreamState::default();
                let mut stderr_progress = ProgressStreamState::default();
                let mut interval = tokio::time::interval(BACKGROUND_PROGRESS_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await;

                let wait = child.wait();
                tokio::pin!(wait);
                let status = loop {
                    tokio::select! {
                        status = &mut wait => break status,
                        _ = interval.tick() => {
                            if let Some((stdout_chunk, stderr_chunk)) = collect_progress_output(
                                &stdout,
                                &stderr,
                                &mut stdout_progress,
                                &mut stderr_progress,
                            ).await {
                                let exec_output = output_chunks_to_exec_output(
                                    pid,
                                    &workspace_str,
                                    stdout_chunk,
                                    stderr_chunk,
                                );
                                emit_background_progress(
                                    &ctx,
                                    &task_id,
                                    exec_output,
                                    json_hook.as_ref(),
                                    hook.as_ref(),
                                ).await;
                            }
                        }
                    }
                };

                let stdout_read_error = output_reader_error(stdout_reader, "stdout").await;
                let stderr_read_error = output_reader_error(stderr_reader, "stderr").await;
                let stdout_bytes = std::mem::take(&mut *stdout.lock().await);
                let mut stderr_bytes = std::mem::take(&mut *stderr.lock().await);
                if let Some(err) = stdout_read_error {
                    append_output_read_error(&mut stderr_bytes, err);
                }
                if let Some(err) = stderr_read_error {
                    append_output_read_error(&mut stderr_bytes, err);
                }

                let exec_output = match status {
                    Ok(status) => {
                        let mut exec_output = ExecOutput::from_output(
                            pid,
                            Some(Output {
                                status,
                                stdout: stdout_bytes,
                                stderr: stderr_bytes,
                            }),
                            &temp_dir,
                        )
                        .await;
                        exec_output.workspace = Some(workspace_str);
                        exec_output
                    }
                    Err(err) => {
                        let mut error =
                            format!("Failed to execute background process: {err}").into_bytes();
                        if !stderr_bytes.is_empty() {
                            error.push(b'\n');
                            error.extend_from_slice(&stderr_bytes);
                        }
                        output_bytes_to_exec_output(pid, &workspace_str, stdout_bytes, error)
                    }
                };

                emit_background_end(
                    &ctx,
                    task_id,
                    exec_output,
                    json_hook.as_ref(),
                    hook.as_ref(),
                )
                .await;
            });
        }

        Ok(exec_output)
    }
}

#[async_trait]
impl Executor for NativeRuntime {
    fn name(&self) -> &str {
        "shell"
    }

    fn workspace(&self) -> &PathBuf {
        &self.workspace
    }

    fn shell(&self) -> &str {
        #[cfg(not(target_os = "windows"))]
        {
            "sh"
        }

        #[cfg(target_os = "windows")]
        {
            "cmd.exe"
        }
    }

    async fn execute(
        &self,
        ctx: BaseCtx,
        input: ExecArgs,
        envs: HashMap<String, String>,
    ) -> Result<ExecOutput, BoxError> {
        let cmd = Self::build_shell_command(&input.command);
        self.execute_command(ctx, self.name(), cmd, envs, Some(input))
            .await
    }
}

#[derive(Default)]
struct ProgressStreamState {
    sent_len: usize,
    terminal: TerminalProgressState,
}

#[derive(Default)]
struct TerminalProgressState {
    line: String,
    cursor: usize,
}

impl ProgressStreamState {
    fn next_output(&mut self, output: &[u8]) -> Option<String> {
        if output.len() <= self.sent_len {
            return None;
        }

        let unread = &output[self.sent_len..];
        let readable_len = complete_utf8_prefix_len(unread);
        if readable_len == 0 {
            return None;
        }

        self.sent_len += readable_len;
        let text = String::from_utf8_lossy(&unread[..readable_len]);
        let progress = self.terminal.render(&text);
        (!progress.is_empty()).then_some(progress)
    }
}

impl TerminalProgressState {
    fn render(&mut self, text: &str) -> String {
        if has_rewrite_control(text) {
            self.render_terminal_text(text)
        } else {
            self.update_plain_text(text);
            text.to_string()
        }
    }

    fn update_plain_text(&mut self, text: &str) {
        for ch in text.chars() {
            if ch == '\n' {
                self.line.clear();
                self.cursor = 0;
            } else {
                self.write_char(ch);
            }
        }
    }

    fn render_terminal_text(&mut self, text: &str) -> String {
        let mut output = String::new();
        let mut chars = text.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '\r' => self.cursor = 0,
                '\n' => {
                    output.push_str(self.line.trim_end_matches(' '));
                    output.push('\n');
                    self.line.clear();
                    self.cursor = 0;
                }
                '\x08' => self.move_cursor_left(),
                '\x1b' => {
                    self.apply_escape_sequence(&mut chars);
                }
                _ => self.write_char(ch),
            }
        }

        let line = self.line.trim_end_matches(' ');
        if !line.is_empty() {
            output.push_str(line);
        }
        output
    }

    fn write_char(&mut self, ch: char) {
        if self.cursor >= self.line.len() {
            self.line.push(ch);
            self.cursor = self.line.len();
            return;
        }

        let end = self.line[self.cursor..]
            .char_indices()
            .nth(1)
            .map(|(idx, _)| self.cursor + idx)
            .unwrap_or(self.line.len());
        let mut buf = [0; 4];
        self.line
            .replace_range(self.cursor..end, ch.encode_utf8(&mut buf));
        self.cursor += ch.len_utf8();
    }

    fn move_cursor_left(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.cursor = self.line[..self.cursor]
            .char_indices()
            .next_back()
            .map(|(idx, _)| idx)
            .unwrap_or(0);
    }

    fn apply_escape_sequence<I>(&mut self, chars: &mut std::iter::Peekable<I>)
    where
        I: Iterator<Item = char>,
    {
        match chars.peek() {
            Some('[') => {
                chars.next();
                let mut command = None;
                for ch in chars.by_ref() {
                    if ('@'..='~').contains(&ch) {
                        command = Some(ch);
                        break;
                    }
                }
                if matches!(command, Some('K')) {
                    self.line.truncate(self.cursor);
                }
            }
            Some(']') => {
                chars.next();
                while let Some(ch) = chars.next() {
                    if ch == '\x07' {
                        break;
                    }
                    if ch == '\x1b' && matches!(chars.peek(), Some('\\')) {
                        chars.next();
                        break;
                    }
                }
            }
            _ => {}
        }
    }
}

fn spawn_output_reader<R>(
    reader: Option<R>,
    output: OutputBuffer,
) -> tokio::task::JoinHandle<std::io::Result<()>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let Some(mut reader) = reader else {
            return Ok(());
        };
        let mut chunk = [0; OUTPUT_READ_CHUNK_BYTES];
        loop {
            let len = reader.read(&mut chunk).await?;
            if len == 0 {
                return Ok(());
            }
            output.lock().await.extend_from_slice(&chunk[..len]);
        }
    })
}

async fn collect_progress_output(
    stdout: &OutputBuffer,
    stderr: &OutputBuffer,
    stdout_progress: &mut ProgressStreamState,
    stderr_progress: &mut ProgressStreamState,
) -> Option<(String, String)> {
    let stdout_chunk = {
        let stdout = stdout.lock().await;
        stdout_progress.next_output(&stdout).unwrap_or_default()
    };

    let stderr_chunk = {
        let stderr = stderr.lock().await;
        stderr_progress.next_output(&stderr).unwrap_or_default()
    };

    if stdout_chunk.is_empty() && stderr_chunk.is_empty() {
        None
    } else {
        Some((stdout_chunk, stderr_chunk))
    }
}

fn complete_utf8_prefix_len(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        return 0;
    }

    let mut continuation_start = bytes.len();
    while continuation_start > 0 && is_utf8_continuation_byte(bytes[continuation_start - 1]) {
        continuation_start -= 1;
    }

    if continuation_start == 0 {
        return bytes.len();
    }

    let lead_index = if continuation_start == bytes.len() {
        bytes.len() - 1
    } else {
        continuation_start - 1
    };
    let required_len = utf8_sequence_len(bytes[lead_index]);
    if required_len > 1 && bytes.len() - lead_index < required_len {
        lead_index
    } else {
        bytes.len()
    }
}

fn is_utf8_continuation_byte(byte: u8) -> bool {
    byte & 0b1100_0000 == 0b1000_0000
}

fn utf8_sequence_len(byte: u8) -> usize {
    if byte & 0b1000_0000 == 0 {
        1
    } else if byte & 0b1110_0000 == 0b1100_0000 {
        2
    } else if byte & 0b1111_0000 == 0b1110_0000 {
        3
    } else if byte & 0b1111_1000 == 0b1111_0000 {
        4
    } else {
        1
    }
}

fn has_rewrite_control(text: &str) -> bool {
    text.contains(['\r', '\x08', '\x1b'])
}

async fn output_reader_error(
    handle: tokio::task::JoinHandle<std::io::Result<()>>,
    stream_name: &str,
) -> Option<String> {
    match handle.await {
        Ok(Ok(())) => None,
        Ok(Err(err)) => Some(format!("Failed to read background {stream_name}: {err}")),
        Err(err) => Some(format!(
            "Failed to join background {stream_name} reader: {err}"
        )),
    }
}

fn append_output_read_error(stderr: &mut Vec<u8>, err: String) {
    if !stderr.is_empty() && !stderr.ends_with(b"\n") {
        stderr.push(b'\n');
    }
    stderr.extend_from_slice(err.as_bytes());
}

fn output_chunks_to_exec_output(
    process_id: Option<u32>,
    workspace: &str,
    stdout: String,
    stderr: String,
) -> ExecOutput {
    ExecOutput {
        workspace: Some(workspace.to_string()),
        process_id,
        stdout: (!stdout.is_empty()).then_some(stdout),
        stderr: (!stderr.is_empty()).then_some(stderr),
        ..Default::default()
    }
}

fn output_bytes_to_exec_output(
    process_id: Option<u32>,
    workspace: &str,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) -> ExecOutput {
    output_chunks_to_exec_output(
        process_id,
        workspace,
        String::from_utf8_lossy(&stdout).to_string(),
        String::from_utf8_lossy(&stderr).to_string(),
    )
}

async fn emit_background_progress(
    ctx: &BaseCtx,
    task_id: &str,
    output: ExecOutput,
    json_hook: Option<&DynToolJsonHook>,
    hook: Option<&ShellToolHook>,
) {
    if let Some(hook) = json_hook {
        hook.on_background_progress(ctx, task_id.to_string(), ToolOutput::new(json!(output)))
            .await;
        return;
    }
    if let Some(hook) = hook {
        hook.on_background_progress(ctx, task_id.to_string(), ToolOutput::new(output))
            .await;
    }
}

async fn emit_background_end(
    ctx: &BaseCtx,
    task_id: String,
    output: ExecOutput,
    json_hook: Option<&DynToolJsonHook>,
    hook: Option<&ShellToolHook>,
) {
    if let Some(hook) = json_hook {
        hook.on_background_end(ctx, task_id, ToolOutput::new(json!(output)))
            .await;
        return;
    }
    if let Some(hook) = hook {
        hook.on_background_end(ctx, task_id, ToolOutput::new(output))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineBuilder;
    use std::{
        path::Path,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio::sync::{mpsc, oneshot};

    struct TestTempDir(PathBuf);

    impl TestTempDir {
        async fn new(prefix: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("{prefix}-{:016x}", rand::random::<u64>()));
            tokio::fs::create_dir_all(&path).await.unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        async fn create_dir(&self, relative: &str) -> PathBuf {
            let path = self.0.join(relative);
            tokio::fs::create_dir_all(&path).await.unwrap();
            path
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[allow(clippy::type_complexity)]
    struct TestHook {
        sender: Mutex<Option<oneshot::Sender<(String, ToolOutput<ExecOutput>)>>>,
    }

    impl TestHook {
        fn new(sender: oneshot::Sender<(String, ToolOutput<ExecOutput>)>) -> Self {
            Self {
                sender: Mutex::new(Some(sender)),
            }
        }
    }

    #[async_trait]
    impl ToolHook<ExecArgs, ExecOutput> for TestHook {
        async fn on_background_end(
            &self,
            _ctx: &BaseCtx,
            task_id: String,
            output: ToolOutput<ExecOutput>,
        ) {
            if let Some(sender) = self.sender.lock().unwrap().take() {
                let _ = sender.send((task_id, output));
            }
        }
    }

    #[allow(clippy::type_complexity)]
    struct ProgressHook {
        progress_sender: mpsc::UnboundedSender<(String, ToolOutput<ExecOutput>)>,
        end_sender: Mutex<Option<oneshot::Sender<(String, ToolOutput<ExecOutput>)>>>,
    }

    impl ProgressHook {
        fn new(
            progress_sender: mpsc::UnboundedSender<(String, ToolOutput<ExecOutput>)>,
            end_sender: oneshot::Sender<(String, ToolOutput<ExecOutput>)>,
        ) -> Self {
            Self {
                progress_sender,
                end_sender: Mutex::new(Some(end_sender)),
            }
        }
    }

    #[async_trait]
    impl ToolHook<ExecArgs, ExecOutput> for ProgressHook {
        async fn on_background_progress(
            &self,
            _ctx: &BaseCtx,
            task_id: String,
            output: ToolOutput<ExecOutput>,
        ) {
            let _ = self.progress_sender.send((task_id, output));
        }

        async fn on_background_end(
            &self,
            _ctx: &BaseCtx,
            task_id: String,
            output: ToolOutput<ExecOutput>,
        ) {
            if let Some(sender) = self.end_sender.lock().unwrap().take() {
                let _ = sender.send((task_id, output));
            }
        }
    }

    fn foreground_command(runtime: &NativeRuntime, env_name: &str, output_file: &str) -> String {
        match runtime.shell() {
            "cmd.exe" => format!(
                "<nul set /p =%{env_name}% > {output_file} & <nul set /p =done & echo warn 1>&2"
            ),
            _ => format!(
                "printf '%s' \"${env_name}\" > {output_file}; printf '%s' 'done'; printf '%s' 'warn' >&2"
            ),
        }
    }

    fn background_command(runtime: &NativeRuntime) -> String {
        match runtime.shell() {
            "cmd.exe" => {
                "ping 127.0.0.1 -n 2 > nul & <nul set /p =bg-out & echo bg-err 1>&2".to_string()
            }
            _ => "sleep 0.2; printf '%s' 'bg-out'; printf '%s' 'bg-err' >&2".to_string(),
        }
    }

    fn background_progress_command(runtime: &NativeRuntime) -> String {
        match runtime.shell() {
            "cmd.exe" => "echo progress-out & echo progress-err 1>&2 & ping 127.0.0.1 -n 2 > nul & <nul set /p =done".to_string(),
            _ => "printf '%s\n' 'progress-out'; printf '%s\n' 'progress-err' >&2; sleep 0.5; printf '%s' 'done'".to_string(),
        }
    }

    #[test]
    fn progress_stream_waits_for_complete_utf8_sequence() {
        let mut state = ProgressStreamState::default();
        let mut output = vec![0xe4, 0xb8];

        assert_eq!(state.next_output(&output), None);

        output.push(0xad);
        assert_eq!(state.next_output(&output).as_deref(), Some("中"));
    }

    #[test]
    fn progress_stream_normalizes_rewritten_terminal_line() {
        let mut state = ProgressStreamState::default();

        assert_eq!(
            state.next_output(b"10%\r20%\r100%").as_deref(),
            Some("100%")
        );
    }

    #[test]
    fn progress_stream_handles_ansi_clear_line() {
        let mut state = ProgressStreamState::default();

        assert_eq!(
            state.next_output(b"abcdef\rxy\x1b[K").as_deref(),
            Some("xy")
        );
    }

    #[test]
    fn progress_stream_handles_backspace_on_utf8_character() {
        let mut state = ProgressStreamState::default();

        assert_eq!(
            state.next_output("中\x08文".as_bytes()).as_deref(),
            Some("文")
        );
    }

    #[test]
    fn new_initializes_paths_and_shell() {
        let runtime = NativeRuntime::new(PathBuf::from("/home/anda-native-runtime-tests"));

        assert_eq!(runtime.name(), "shell");
        assert_eq!(
            runtime.workspace(),
            &PathBuf::from("/home/anda-native-runtime-tests")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_runs_foreground_command_with_envs_and_workspace() {
        let ctx = EngineBuilder::new().mock_ctx();
        let workspace = TestTempDir::new("anda-native-foreground").await;
        let nested_dir = workspace.create_dir("nested").await;
        let runtime = NativeRuntime::new(nested_dir.clone());
        let env_name = "ANDA_NATIVE_TEST_VALUE";
        let output_file = "env.txt";
        let mut envs = HashMap::new();
        envs.insert(env_name.to_string(), "secret-value".to_string());

        let output = runtime
            .execute(
                ctx.base,
                ExecArgs {
                    command: foreground_command(&runtime, env_name, output_file),
                    ..Default::default()
                },
                envs,
            )
            .await
            .unwrap();

        let written = tokio::fs::read_to_string(nested_dir.join(output_file))
            .await
            .unwrap();
        assert_eq!(written.trim(), "secret-value");
        assert!(output.process_id.is_some());
        assert!(output.raw_output_path.is_none());
        assert_eq!(output.stdout.as_deref().map(str::trim), Some("done"));
        assert_eq!(output.stderr.as_deref().map(str::trim), Some("warn"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_reports_background_output_via_hook() {
        let ctx = EngineBuilder::new().mock_ctx();
        let workspace = TestTempDir::new("anda-native-background").await;
        let (sender, receiver) = oneshot::channel();
        let hook = ShellToolHook::new(Arc::new(TestHook::new(sender)));
        ctx.base.set_state(hook);
        let runtime = NativeRuntime::new(workspace.path().to_path_buf());
        let input = ExecArgs {
            command: background_command(&runtime),
            background: true,
            ..Default::default()
        };

        let output = runtime
            .execute(ctx.base, input.clone(), HashMap::new())
            .await
            .unwrap();

        assert!(output.process_id.is_some());
        assert!(output.exit_status.is_some());
        assert!(output.stdout.is_some());
        assert!(output.stderr.is_none());

        let (
            task_id,
            ToolOutput {
                output: hook_output,
                ..
            },
        ) = tokio::time::timeout(Duration::from_secs(5), receiver)
            .await
            .unwrap()
            .unwrap();

        assert!(task_id.contains("shell"));
        assert_eq!(hook_output.process_id, output.process_id);
        assert_eq!(hook_output.stdout.as_deref().map(str::trim), Some("bg-out"));
        assert_eq!(hook_output.stderr.as_deref().map(str::trim), Some("bg-err"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execute_reports_background_progress_via_hook() {
        let ctx = EngineBuilder::new().mock_ctx();
        let workspace = TestTempDir::new("anda-native-progress").await;
        let (progress_sender, mut progress_receiver) = mpsc::unbounded_channel();
        let (end_sender, end_receiver) = oneshot::channel();
        let hook = ShellToolHook::new(Arc::new(ProgressHook::new(progress_sender, end_sender)));
        ctx.base.set_state(hook);
        let runtime = NativeRuntime::new(workspace.path().to_path_buf());
        let input = ExecArgs {
            command: background_progress_command(&runtime),
            background: true,
            ..Default::default()
        };

        let output = runtime
            .execute(ctx.base, input.clone(), HashMap::new())
            .await
            .unwrap();

        assert!(output.process_id.is_some());
        assert!(output.exit_status.is_some());
        assert!(output.stdout.is_some());
        assert!(output.stderr.is_none());

        let progress_task_id = tokio::time::timeout(Duration::from_secs(5), async {
            let mut saw_stdout = false;
            let mut saw_stderr = false;
            loop {
                let (
                    task_id,
                    ToolOutput {
                        output: progress_output,
                        ..
                    },
                ) = progress_receiver.recv().await.unwrap();
                assert_eq!(progress_output.process_id, output.process_id);
                assert!(progress_output.exit_status.is_none());
                if progress_output
                    .stdout
                    .as_deref()
                    .is_some_and(|stdout| stdout.contains("progress-out"))
                {
                    saw_stdout = true;
                }
                if progress_output
                    .stderr
                    .as_deref()
                    .is_some_and(|stderr| stderr.contains("progress-err"))
                {
                    saw_stderr = true;
                }
                if saw_stdout && saw_stderr {
                    break task_id;
                }
            }
        })
        .await
        .unwrap();

        let (
            end_task_id,
            ToolOutput {
                output: hook_output,
                ..
            },
        ) = tokio::time::timeout(Duration::from_secs(5), end_receiver)
            .await
            .unwrap()
            .unwrap();

        assert!(progress_task_id.contains("shell"));
        assert_eq!(end_task_id, progress_task_id);
        assert_eq!(hook_output.process_id, output.process_id);
        assert!(
            hook_output
                .stdout
                .as_deref()
                .is_some_and(|stdout| stdout.contains("progress-out") && stdout.contains("done"))
        );
        assert!(
            hook_output
                .stderr
                .as_deref()
                .is_some_and(|stderr| stderr.contains("progress-err"))
        );
    }
}
