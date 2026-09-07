//! Discord voice ingest worker + host heartbeat for delegated Stream Sync sessions.

use crate::app_state::{AppState, DiscordVoiceLastWrite};
use crate::delegated_lifecycle::{
    clear_finished_generation_task, connection_key_authorization, generation_task_alive,
    install_generation_task, release_generation_slot_if_owned,
};
use crate::twitch::TwitchServices;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Local};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const IDLE_FINALIZE_AFTER: Duration = Duration::from_secs(20);
const DEFAULT_POLL_WAIT: Duration = Duration::from_millis(1000);
const RETRY_AFTER_MAX: Duration = Duration::from_secs(60);
const MIN_LOOP_WAIT: Duration = Duration::from_millis(200);
const PCM_BYTES_PER_MS: u64 = 192;
const WAV_HEADER_SIZE: u64 = 44;
const SILENCE_WRITE_CHUNK_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone)]
struct PendingChunk {
    chunk_id: String,
    is_silence: bool,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    started_at: String,
    stopped_at: Option<String>,
    guild_name: String,
    channel_name: String,
    nick: String,
}

#[derive(Debug, Clone)]
struct PendingPoll {
    retry_after: Duration,
    chunk: Option<PendingChunk>,
}

#[derive(Debug, Deserialize)]
struct HostStateAck {
    #[serde(default)]
    ok: bool,
}

pub async fn status_json(state: &AppState) -> Value {
    let cfg = state.discord_voice_config.read().await.clone();
    let runtime = state.discord_voice_runtime.read().await.clone();
    let parent = cfg.recording_parent.clone();
    json!({
        "deviceId": cfg.device_id,
        "parentFolderSet": parent.as_ref().is_some_and(|v| !v.trim().is_empty()),
        "parentFolderPath": parent,
        "parentFolderLabel": parent.as_ref().map(|v| parent_folder_label(v)),
        "discordConnected": cfg.host_token.as_ref().is_some_and(|v| v.starts_with("sdk_")),
        "lastError": runtime.last_error,
        "lastWrite": runtime.last_write.as_ref().map(|w| {
            json!({
                "at": w.at,
                "path": w.path,
                "bytes": w.bytes,
            })
        }),
    })
}

pub async fn save_recording_parent_folder(state: Arc<AppState>, path: &str) -> Result<()> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("Pick a recording folder first."));
    }
    if trimmed.contains('\0') {
        return Err(anyhow!("Recording folder path is invalid."));
    }
    {
        let mut cfg = state.discord_voice_config.write().await;
        cfg.recording_parent = Some(trimmed.to_string());
    }
    state.save_discord_voice_config().await?;
    clear_last_error(&state).await;
    Ok(())
}

pub async fn redeem_connect_key(
    state: Arc<AppState>,
    key: &str,
    requested_device_id: Option<&str>,
) -> Result<()> {
    let trimmed = key.trim();
    if !trimmed.starts_with("sdk_") {
        return Err(anyhow!("Paste the key Discord showed after /connect."));
    }
    let stored = state.discord_voice_config.read().await.clone();
    let device_id = requested_device_id
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(stored.device_id.as_str())
        .to_string();
    if device_id.trim().is_empty() {
        return Err(anyhow!(
            "Missing local device id for Discord connect."
        ));
    }
    let url = format!(
        "{}/api/stream-sync/discord-connect-keys/redeem",
        crate::syndicate_connection::api_base()
    );
    let res = crate::syndicate_connection::syndicate_http_client()
        .post(url)
        .header("Content-Type", "application/json")
        .json(&json!({
            "key": trimmed,
            "device_id": device_id,
        }))
        .send()
        .await
        .map_err(|e| anyhow!("Discord connect request failed: {e}"))?;

    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap_or_else(|_| json!({}));
    if status < 200 || status >= 300 || body.get("ok").and_then(|v| v.as_bool()) == Some(false) {
        let detail = body
            .get("message")
            .or_else(|| body.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Discord connect failed.");
        return Err(anyhow!(detail.to_string()));
    }
    {
        let mut cfg = state.discord_voice_config.write().await;
        cfg.host_token = Some(trimmed.to_string());
        if cfg.device_id.trim().is_empty() {
            cfg.device_id = device_id;
        }
    }
    state.save_discord_voice_config().await?;
    clear_last_error(&state).await;
    Ok(())
}

pub async fn maybe_autostart(state: Arc<AppState>, services: Arc<TwitchServices>) {
    ensure_ingest_worker(state, services).await;
}

pub async fn ensure_ingest_worker(state: Arc<AppState>, services: Arc<TwitchServices>) {
    let has_token = state
        .discord_voice_config
        .read()
        .await
        .host_token
        .as_ref()
        .is_some_and(|v| v.starts_with("sdk_"));
    if !has_token {
        return;
    }
    let generation = 1;
    clear_finished_generation_task(&services.discord_voice_handle, generation).await;
    let running = services
        .discord_voice_handle
        .read()
        .await
        .as_ref()
        .is_some_and(|t| generation_task_alive(t, generation));
    if running {
        return;
    }
    let (grant_tx, grant_rx) = oneshot::channel();
    let state2 = state.clone();
    let services2 = services.clone();
    let handle = tokio::spawn(async move {
        if grant_rx.await.is_err() {
            return;
        }
        ingest_loop(state2.clone()).await;
        release_generation_slot_if_owned(&services2.discord_voice_handle, generation).await;
    });
    if install_generation_task(&services.discord_voice_handle, generation, handle).await {
        let _ = grant_tx.send(());
    }
}

pub async fn stop_ingest_worker_for_generation(
    services: &TwitchServices,
    generation: crate::delegated_lifecycle::DelegatedGeneration,
) {
    let mut guard = services.discord_voice_handle.write().await;
    let Some(task) = guard.take() else {
        return;
    };
    if generation == 0 || task.generation == generation {
        task.handle.abort();
    } else {
        *guard = Some(task);
    }
}

async fn ingest_loop(state: Arc<AppState>) {
    let mut next_heartbeat = Instant::now();
    let mut active_files: HashMap<PathBuf, Instant> = HashMap::new();
    loop {
        let cfg = state.discord_voice_config.read().await.clone();
        let Some(key) = cfg
            .host_token
            .clone()
            .filter(|v| v.starts_with("sdk_"))
        else {
            break;
        };
        let parent_folder = cfg
            .recording_parent
            .clone()
            .filter(|v| !v.trim().is_empty());
        let parent_label = parent_folder.as_ref().map(|p| parent_folder_label(p));
        let device_id = cfg.device_id.clone();

        if Instant::now() >= next_heartbeat {
            if let Err(e) = post_host_state(
                &key,
                &device_id,
                parent_folder.is_some(),
                parent_label.as_deref(),
            )
            .await
            {
                set_last_error(&state, e.to_string()).await;
            }
            next_heartbeat = Instant::now() + HEARTBEAT_INTERVAL;
        }

        let wait = if let Some(parent) = parent_folder {
            match poll_pending_chunk(&key).await {
                Ok(pending) => {
                    let mut next_wait = pending.retry_after;
                    if let Some(chunk) = pending.chunk {
                        match process_chunk(&key, Path::new(&parent), &chunk).await {
                            Ok((target_path, bytes_written)) => {
                                clear_last_error(&state).await;
                                set_last_write(&state, &target_path, bytes_written).await;
                                active_files.insert(target_path.clone(), Instant::now());
                                if chunk
                                    .stopped_at
                                    .as_ref()
                                    .is_some_and(|v| !v.trim().is_empty())
                                {
                                    let _ = finalize_wav_file(&target_path);
                                    active_files.remove(&target_path);
                                }
                            }
                            Err(err) => {
                                set_last_error(&state, err.to_string()).await;
                                next_wait = Duration::from_secs(2);
                            }
                        }
                    } else {
                        finalize_idle_files(&mut active_files);
                    }
                    next_wait
                }
                Err(err) => {
                    set_last_error(&state, err.to_string()).await;
                    finalize_idle_files(&mut active_files);
                    Duration::from_secs(2)
                }
            }
        } else {
            set_last_error(
                &state,
                "Set a recording folder to start Discord voice ingest.".into(),
            )
            .await;
            finalize_idle_files(&mut active_files);
            Duration::from_secs(2)
        };

        let until_heartbeat = next_heartbeat.saturating_duration_since(Instant::now());
        let sleep_for = normalize_retry_after(wait).min(until_heartbeat.max(MIN_LOOP_WAIT));
        tokio::time::sleep(sleep_for).await;
    }
    finalize_all_files(&active_files);
}

async fn post_host_state(
    key: &str,
    device_id: &str,
    parent_folder_set: bool,
    parent_folder_label: Option<&str>,
) -> Result<()> {
    let url = format!(
        "{}/api/stream-sync/voice/host-state",
        crate::syndicate_connection::api_base()
    );
    let res = crate::syndicate_connection::syndicate_http_client()
        .post(url)
        .header("Authorization", connection_key_authorization(key))
        .header("Content-Type", "application/json")
        .json(&json!({
            "device_id": device_id,
            "state": "online",
            "version": env!("CARGO_PKG_VERSION"),
            "parent_folder_set": parent_folder_set,
            "parent_folder_label": parent_folder_label.unwrap_or(""),
        }))
        .send()
        .await
        .context("voice host heartbeat request failed")?;
    if !res.status().is_success() {
        let code = res.status().as_u16();
        let body: Value = res.json().await.unwrap_or_else(|_| json!({}));
        let msg = body
            .get("message")
            .or_else(|| body.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("voice host heartbeat failed");
        return Err(anyhow!("voice host heartbeat HTTP {code}: {msg}"));
    }
    let _ = res
        .json::<HostStateAck>()
        .await
        .ok()
        .map(|ack| ack.ok)
        .unwrap_or(true);
    Ok(())
}

async fn poll_pending_chunk(key: &str) -> Result<PendingPoll> {
    let url = format!(
        "{}/api/stream-sync/voice/chunks/pending?limit=1",
        crate::syndicate_connection::api_base()
    );
    let res = crate::syndicate_connection::syndicate_http_client()
        .get(url)
        .header("Authorization", connection_key_authorization(key))
        .header("Accept", "application/json")
        .send()
        .await
        .context("voice chunk poll request failed")?;

    if res.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(PendingPoll {
            retry_after: DEFAULT_POLL_WAIT,
            chunk: None,
        });
    }

    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap_or_else(|_| json!({}));
    if status < 200 || status >= 300 {
        let msg = body
            .get("message")
            .or_else(|| body.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("voice chunk poll failed");
        return Err(anyhow!("voice chunk poll HTTP {status}: {msg}"));
    }

    let retry_after = retry_after_from_value(&body);
    let chunk = first_chunk_value(&body)
        .map(parse_pending_chunk)
        .transpose()?;
    Ok(PendingPoll { retry_after, chunk })
}

async fn process_chunk(
    key: &str,
    parent_folder: &Path,
    chunk: &PendingChunk,
) -> Result<(PathBuf, u64)> {
    let path = build_target_path(parent_folder, chunk)?;
    let bytes_written = if chunk.is_silence {
        let duration_ms = silence_duration_ms(chunk.start_ms, chunk.end_ms);
        append_silence_chunk(&path, duration_ms)?
    } else {
        append_content_chunk(&path, key, &chunk.chunk_id).await?
    };
    ack_chunk(key, &chunk.chunk_id).await?;
    Ok((path, bytes_written))
}

async fn append_content_chunk(path: &Path, key: &str, chunk_id: &str) -> Result<u64> {
    ensure_wav_file(path)?;
    let url = format!(
        "{}/api/stream-sync/voice/chunks/{}/content",
        crate::syndicate_connection::api_base(),
        urlencoding::encode(chunk_id)
    );
    let res = crate::syndicate_connection::syndicate_http_client()
        .get(url)
        .header("Authorization", connection_key_authorization(key))
        .send()
        .await
        .context("voice chunk content request failed")?;
    if !res.status().is_success() {
        return Err(anyhow!(
            "voice chunk content HTTP {}",
            res.status().as_u16()
        ));
    }
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .with_context(|| format!("open voice wav append {}", path.display()))?;
    let mut written = 0u64;
    let mut stream = res.bytes_stream();
    while let Some(next) = stream.next().await {
        let bytes = next.context("voice chunk content stream read failed")?;
        if bytes.is_empty() {
            continue;
        }
        file.write_all(&bytes)
            .with_context(|| format!("append voice wav {}", path.display()))?;
        written = written.saturating_add(bytes.len() as u64);
    }
    file.flush()
        .with_context(|| format!("flush voice wav {}", path.display()))?;
    Ok(written)
}

fn append_silence_chunk(path: &Path, duration_ms: u64) -> Result<u64> {
    ensure_wav_file(path)?;
    let total = silence_bytes_for_duration_ms(duration_ms);
    if total == 0 {
        return Ok(0);
    }
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .with_context(|| format!("open silence append {}", path.display()))?;
    let zero = vec![0u8; SILENCE_WRITE_CHUNK_BYTES];
    let mut remaining = total;
    while remaining > 0 {
        let step = remaining.min(zero.len() as u64) as usize;
        file.write_all(&zero[..step])
            .with_context(|| format!("write silence {}", path.display()))?;
        remaining -= step as u64;
    }
    file.flush()
        .with_context(|| format!("flush silence wav {}", path.display()))?;
    Ok(total)
}

async fn ack_chunk(key: &str, chunk_id: &str) -> Result<()> {
    let url = format!(
        "{}/api/stream-sync/voice/chunks/ack",
        crate::syndicate_connection::api_base()
    );
    let res = crate::syndicate_connection::syndicate_http_client()
        .post(url)
        .header("Authorization", connection_key_authorization(key))
        .header("Content-Type", "application/json")
        .json(&json!({
            "acks": [{ "chunk_id": chunk_id }],
        }))
        .send()
        .await
        .context("voice chunk ack request failed")?;
    if !res.status().is_success() {
        return Err(anyhow!("voice chunk ack HTTP {}", res.status().as_u16()));
    }
    Ok(())
}

fn parse_pending_chunk(value: &Value) -> Result<PendingChunk> {
    let chunk_id = find_string(
        value,
        &[
            "/chunk_id",
            "/chunkId",
            "/id",
            "/chunk/id",
            "/voice_chunk_id",
        ],
    )
    .ok_or_else(|| anyhow!("pending voice chunk missing chunk id"))?;
    let is_silence =
        find_bool(value, &["/is_silence", "/isSilence", "/chunk/is_silence"]).unwrap_or(false);
    let start_ms = find_i64(
        value,
        &[
            "/start_ms",
            "/startMs",
            "/range/start_ms",
            "/range/startMs",
            "/start",
        ],
    );
    let end_ms = find_i64(
        value,
        &["/end_ms", "/endMs", "/range/end_ms", "/range/endMs", "/end"],
    );
    let started_at = find_string(
        value,
        &[
            "/started_at",
            "/startedAt",
            "/session/started_at",
            "/session/startedAt",
        ],
    )
    .ok_or_else(|| anyhow!("pending voice chunk missing session started_at"))?;
    let stopped_at = find_string(
        value,
        &[
            "/stopped_at",
            "/stoppedAt",
            "/session/stopped_at",
            "/session/stoppedAt",
        ],
    );
    let guild_name = find_string(
        value,
        &[
            "/guild_name",
            "/guildName",
            "/guild/name",
            "/session/guild_name",
            "/session/guild/name",
        ],
    )
    .unwrap_or_else(|| "Discord Guild".into());
    let channel_name = find_string(
        value,
        &[
            "/channel_name",
            "/channelName",
            "/channel/name",
            "/session/channel_name",
            "/session/channel/name",
        ],
    )
    .unwrap_or_else(|| "Voice Channel".into());
    let nick = find_string(
        value,
        &[
            "/nick",
            "/display_name",
            "/user_nick",
            "/member_nick",
            "/speaker_nick",
            "/speaker/nick",
            "/speaker/display_name",
            "/user/nick",
            "/participant/nick",
        ],
    )
    .unwrap_or_else(|| "Speaker".into());
    Ok(PendingChunk {
        chunk_id,
        is_silence,
        start_ms,
        end_ms,
        started_at,
        stopped_at,
        guild_name,
        channel_name,
        nick,
    })
}

fn first_chunk_value<'a>(body: &'a Value) -> Option<&'a Value> {
    body.get("chunks")
        .and_then(Value::as_array)
        .and_then(|arr| arr.first())
        .or_else(|| body.get("chunk"))
}

fn retry_after_from_value(body: &Value) -> Duration {
    let ms = body
        .get("retry_after_ms")
        .or_else(|| body.get("retryAfterMs"))
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_POLL_WAIT.as_millis() as u64);
    Duration::from_millis(ms.max(MIN_LOOP_WAIT.as_millis() as u64))
}

fn normalize_retry_after(wait: Duration) -> Duration {
    wait.max(MIN_LOOP_WAIT).min(RETRY_AFTER_MAX)
}

fn find_string(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers
        .iter()
        .filter_map(|path| value.pointer(path).and_then(Value::as_str))
        .map(str::trim)
        .find(|v| !v.is_empty())
        .map(|v| v.to_string())
}

fn find_bool(value: &Value, pointers: &[&str]) -> Option<bool> {
    pointers
        .iter()
        .filter_map(|path| value.pointer(path).and_then(Value::as_bool))
        .next()
}

fn find_i64(value: &Value, pointers: &[&str]) -> Option<i64> {
    for path in pointers {
        let Some(v) = value.pointer(path) else {
            continue;
        };
        if let Some(i) = v.as_i64() {
            return Some(i);
        }
        if let Some(u) = v.as_u64() {
            return Some(u as i64);
        }
    }
    None
}

fn build_target_path(parent_folder: &Path, chunk: &PendingChunk) -> Result<PathBuf> {
    let parent = PathBuf::from(parent_folder);
    let guild = sanitize_segment(&chunk.guild_name, "Discord Guild");
    let channel = sanitize_segment(&chunk.channel_name, "Voice Channel");
    let nick = sanitize_segment(&chunk.nick, "Speaker");
    let stamp = format_session_stamp_local(&chunk.started_at)?;
    let session_folder = format!("{channel}-{stamp}");
    let target = parent
        .join(guild)
        .join(session_folder)
        .join(format!("{nick}.wav"));
    if !target.starts_with(&parent) {
        return Err(anyhow!("recording path escaped parent folder"));
    }
    Ok(target)
}

fn format_session_stamp_local(started_at: &str) -> Result<String> {
    let parsed = DateTime::parse_from_rfc3339(started_at)
        .with_context(|| format!("invalid session started_at: {started_at}"))?;
    Ok(parsed
        .with_timezone(&Local)
        .format("%d%m%Y%H%M%S")
        .to_string())
}

fn sanitize_segment(raw: &str, fallback: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        let clean = if ch.is_control()
            || matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
        {
            '_'
        } else {
            ch
        };
        out.push(clean);
    }
    let mut compact = out
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(['.', ' '])
        .to_string();
    while compact.contains("..") {
        compact = compact.replace("..", "_");
    }
    if compact.is_empty() {
        fallback.to_string()
    } else {
        compact
    }
}

fn ensure_wav_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("mkdir voice path {}", parent.display()))?;
    }
    if !path.exists() {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .with_context(|| format!("create wav file {}", path.display()))?;
        file.write_all(&wav_header(0))
            .with_context(|| format!("write wav header {}", path.display()))?;
        file.flush()
            .with_context(|| format!("flush wav header {}", path.display()))?;
        return Ok(());
    }
    let len = std::fs::metadata(path)
        .with_context(|| format!("stat wav file {}", path.display()))?
        .len();
    if len < WAV_HEADER_SIZE {
        return Err(anyhow!("existing wav file is shorter than header"));
    }
    Ok(())
}

fn finalize_wav_file(path: &Path) -> Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open wav finalize {}", path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("metadata wav {}", path.display()))?
        .len();
    if len < WAV_HEADER_SIZE {
        return Ok(());
    }
    let data_len = (len - WAV_HEADER_SIZE).min(u32::MAX as u64) as u32;
    let riff_len = data_len.saturating_add(36);
    file.seek(SeekFrom::Start(4))
        .with_context(|| format!("seek riff size {}", path.display()))?;
    file.write_all(&riff_len.to_le_bytes())
        .with_context(|| format!("write riff size {}", path.display()))?;
    file.seek(SeekFrom::Start(40))
        .with_context(|| format!("seek data size {}", path.display()))?;
    file.write_all(&data_len.to_le_bytes())
        .with_context(|| format!("write data size {}", path.display()))?;
    file.flush()
        .with_context(|| format!("flush wav finalize {}", path.display()))?;
    Ok(())
}

fn finalize_idle_files(active_files: &mut HashMap<PathBuf, Instant>) {
    let now = Instant::now();
    let stale: Vec<PathBuf> = active_files
        .iter()
        .filter_map(|(path, ts)| {
            (now.saturating_duration_since(*ts) >= IDLE_FINALIZE_AFTER).then_some(path.clone())
        })
        .collect();
    for path in stale {
        let _ = finalize_wav_file(&path);
        active_files.remove(&path);
    }
}

fn finalize_all_files(active_files: &HashMap<PathBuf, Instant>) {
    for path in active_files.keys() {
        let _ = finalize_wav_file(path);
    }
}

fn wav_header(data_len: u32) -> [u8; 44] {
    let sample_rate = 48_000u32;
    let channels = 2u16;
    let bits_per_sample = 16u16;
    let byte_rate = sample_rate * channels as u32 * (bits_per_sample as u32 / 8);
    let block_align = channels * (bits_per_sample / 8);
    let riff_size = data_len.saturating_add(36);
    let mut h = [0u8; 44];
    h[0..4].copy_from_slice(b"RIFF");
    h[4..8].copy_from_slice(&riff_size.to_le_bytes());
    h[8..12].copy_from_slice(b"WAVE");
    h[12..16].copy_from_slice(b"fmt ");
    h[16..20].copy_from_slice(&16u32.to_le_bytes());
    h[20..22].copy_from_slice(&1u16.to_le_bytes());
    h[22..24].copy_from_slice(&channels.to_le_bytes());
    h[24..28].copy_from_slice(&sample_rate.to_le_bytes());
    h[28..32].copy_from_slice(&byte_rate.to_le_bytes());
    h[32..34].copy_from_slice(&block_align.to_le_bytes());
    h[34..36].copy_from_slice(&bits_per_sample.to_le_bytes());
    h[36..40].copy_from_slice(b"data");
    h[40..44].copy_from_slice(&data_len.to_le_bytes());
    h
}

fn silence_duration_ms(start_ms: Option<i64>, end_ms: Option<i64>) -> u64 {
    let start = start_ms.unwrap_or(0);
    let end = end_ms.unwrap_or(start);
    (end - start).max(0) as u64
}

fn silence_bytes_for_duration_ms(duration_ms: u64) -> u64 {
    duration_ms.saturating_mul(PCM_BYTES_PER_MS)
}

fn parent_folder_label(parent_folder: &str) -> String {
    let p = Path::new(parent_folder.trim());
    p.file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| parent_folder.trim().to_string())
}

async fn set_last_error(state: &AppState, message: String) {
    let mut runtime = state.discord_voice_runtime.write().await;
    runtime.last_error = Some(message);
}

async fn clear_last_error(state: &AppState) {
    let mut runtime = state.discord_voice_runtime.write().await;
    runtime.last_error = None;
}

async fn set_last_write(state: &AppState, path: &Path, bytes_written: u64) {
    let mut runtime = state.discord_voice_runtime.write().await;
    runtime.last_write = Some(DiscordVoiceLastWrite {
        at: Some(chrono::Utc::now().to_rfc3339()),
        path: Some(path.display().to_string()),
        bytes: Some(bytes_written),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_sanitization_blocks_escape_and_windows_illegal_chars() {
        let parent = std::path::Path::new("/tmp/recordings");
        let chunk = PendingChunk {
            chunk_id: "c1".into(),
            is_silence: false,
            start_ms: None,
            end_ms: None,
            started_at: "2026-09-07T02:00:00Z".into(),
            stopped_at: None,
            guild_name: "../../guild:bad".into(),
            channel_name: "voice/room".into(),
            nick: "..\\../user*id?".into(),
        };
        let path = build_target_path(parent, &chunk).expect("target path");
        assert!(path.starts_with(parent));
        let text = path.to_string_lossy();
        assert!(!text.contains(".."));
        assert!(!text.contains(':'));
        assert!(!text.contains('*'));
        assert!(text.ends_with(".wav"));
    }

    #[test]
    fn silence_padding_math_uses_192_bytes_per_ms() {
        assert_eq!(silence_bytes_for_duration_ms(0), 0);
        assert_eq!(silence_bytes_for_duration_ms(1), 192);
        assert_eq!(silence_bytes_for_duration_ms(250), 48_000);
        assert_eq!(silence_bytes_for_duration_ms(1_000), 192_000);
        assert_eq!(silence_duration_ms(Some(1200), Some(1700)), 500);
    }
}
