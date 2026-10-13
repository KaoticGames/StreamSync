//! Full HTTP orchestrator E2E: Syndicate API (307 + bearer) → R2 (206, no bearer) → local publish → receipt.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::json;
use sha2::{Digest, Sha256};
use stream_sync_core::voice_delivery::PENDING_PATH;
use stream_sync_core::voice_delivery::{
    compute_finalized_manifest_digest, parse_syndicate_finalized_manifest, FinalizedVoiceDelivery,
    HttpVoiceV2Client, PendingDelivery, VoiceV2ClientError,
};
use stream_sync_core::voice_delivery::{
    DeliveryPhase, OrchestratorError, RECOVERY_REPORT_RELATIVE_PATH,
};

#[derive(Default, Clone)]
struct CapturedRequest {
    method: String,
    path: String,
    auth: Option<String>,
    cookie: Option<String>,
    range: Option<String>,
    control: Option<String>,
    sdk: Option<String>,
}

fn read_http_request(stream: &mut TcpStream) -> (CapturedRequest, Vec<u8>) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = stream.read(&mut tmp).expect("read");
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or("");
    let mut cap = CapturedRequest::default();
    let mut parts = request_line.split_whitespace();
    cap.method = parts.next().unwrap_or("").into();
    cap.path = parts.next().unwrap_or("").into();
    let mut content_length = 0usize;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("authorization:") {
            cap.auth = Some(v.trim().into());
        } else if let Some(v) = lower.strip_prefix("cookie:") {
            cap.cookie = Some(v.trim().into());
        } else if let Some(v) = lower.strip_prefix("range:") {
            cap.range = Some(v.trim().into());
        } else if let Some(v) = lower.strip_prefix("x-stream-sync-control-token:") {
            cap.control = Some(v.trim().into());
        } else if let Some(v) = lower.strip_prefix("x-stream-sync-sdk:") {
            cap.sdk = Some(v.trim().into());
        } else if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(buf.len());
    let body = buf[header_end..].to_vec();
    let mut body_read = body.len();
    while body_read < content_length {
        let n = stream.read(&mut tmp).expect("read body");
        if n == 0 {
            break;
        }
        body_read += n;
    }
    (cap, body)
}

fn write_response(stream: &mut TcpStream, status: &str, headers: &[(&str, &str)], body: &[u8]) {
    let mut out = format!("HTTP/1.1 {status}\r\n");
    for (k, v) in headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    stream.write_all(out.as_bytes()).unwrap();
    if !body.is_empty() {
        stream.write_all(body).unwrap();
    }
    let _ = stream.shutdown(std::net::Shutdown::Write);
}

fn assert_r2_request_clean(req: &CapturedRequest) {
    assert_eq!(req.method, "GET");
    assert!(req.auth.is_none(), "R2 must not receive Authorization");
    assert!(req.cookie.is_none(), "R2 must not receive Cookie");
    assert!(req.control.is_none());
    assert!(req.sdk.is_none());
}

struct MiniServer {
    addr: String,
    _handle: thread::JoinHandle<()>,
}

impl MiniServer {
    fn spawn<F>(handler: F) -> Self
    where
        F: Fn(CapturedRequest, Vec<u8>, &mut TcpStream) + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(false).unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut stream = stream;
                let (req, body) = read_http_request(&mut stream);
                handler(req, body, &mut stream);
            }
        });
        Self {
            addr,
            _handle: handle,
        }
    }
}

struct SyndicateHarness {
    api: MiniServer,
    r2_port: u16,
    receipt_posts: Arc<Mutex<u32>>,
    pending: PendingDelivery,
    report_r2_403_once: Arc<Mutex<bool>>,
}

const MINIMAL_WAV: [u8; 44] = [
    82, 73, 70, 70, 36, 0, 0, 0, 87, 65, 86, 69, 102, 109, 116, 32, 16, 0, 0, 0, 1, 0, 2, 0, 128,
    187, 0, 0, 0, 238, 2, 0, 4, 0, 16, 0, 100, 97, 116, 97, 0, 0, 0, 0,
];

impl SyndicateHarness {
    fn new(stem_bytes: Vec<u8>, report_bytes: Vec<u8>) -> Self {
        Self::with_options(stem_bytes, report_bytes, false)
    }

    fn with_options(stem_bytes: Vec<u8>, report_bytes: Vec<u8>, report_api_404: bool) -> Self {
        let report_sha = format!("{:x}", Sha256::digest(&report_bytes));
        let stem_sha = format!("{:x}", Sha256::digest(&stem_bytes));
        let raw = json!({
            "version": 1,
            "sessionId": "550e8400-e29b-41d4-a716-446655440000",
            "guildId": "123456789012345678",
            "channelId": "234567890123456789",
            "sessionStartWallMs": 1,
            "sessionStartMonotonicNs": "100",
            "sealedStopWallMs": 2,
            "sealedStopMonotonicNs": "200",
            "targetSampleCount48k": 0,
            "journalFormatVersion": 2,
            "journalSha256": "a".repeat(64),
            "stems": [{
                "discordUserId": "123456789012345678",
                "pathNick": "user1",
                "wavRelativePath": "user1.wav",
                "wavBytes": stem_bytes.len(),
                "sha256": stem_sha,
            }],
            "recoveryReport": {
                "relativePath": RECOVERY_REPORT_RELATIVE_PATH,
                "byteLength": report_bytes.len(),
                "sha256": report_sha,
                "contentType": "text/plain",
            }
        });
        let manifest = parse_syndicate_finalized_manifest(&raw).unwrap();
        let digest = compute_finalized_manifest_digest(&manifest);
        let pending = PendingDelivery {
            session_id: manifest.session_id.clone(),
            manifest_digest: digest,
            sealed_at: "2026-01-02T03:04:05.678Z".into(),
            manifest,
        };
        let pending_json = json!({
            "ok": true,
            "deliveries": [{
                "sessionId": pending.session_id,
                "manifestDigest": pending.manifest_digest,
                "sealedAt": pending.sealed_at,
                "manifest": raw,
            }]
        });
        let receipt_posts = Arc::new(Mutex::new(0u32));
        let report_r2_403_once = Arc::new(Mutex::new(false));
        let stem_store = Arc::new(Mutex::new(stem_bytes.clone()));
        let report_store = Arc::new(Mutex::new(report_bytes.clone()));
        let pending_store = Arc::new(Mutex::new(pending_json));
        let receipt_posts2 = receipt_posts.clone();
        let report_r2_403_once2 = report_r2_403_once.clone();

        let r2 = MiniServer::spawn({
            let stem_store = stem_store.clone();
            let report_store = report_store.clone();
            let report_r2_403_once2 = report_r2_403_once2.clone();
            move |req, _body, stream| {
                assert_r2_request_clean(&req);
                if req.path.contains("report") && *report_r2_403_once2.lock().unwrap() {
                    *report_r2_403_once2.lock().unwrap() = false;
                    write_response(stream, "403 Forbidden", &[], b"expired");
                    return;
                }
                let range = req
                    .range
                    .as_deref()
                    .and_then(|r| r.strip_prefix("bytes="))
                    .expect("range");
                let (start, end_inclusive) = range.split_once('-').unwrap();
                let start = start.parse::<usize>().unwrap();
                let end_inclusive = end_inclusive.parse::<usize>().unwrap();
                let len = end_inclusive + 1 - start;
                let data = if req.path.contains("stem") {
                    stem_store.lock().unwrap().clone()
                } else {
                    report_store.lock().unwrap().clone()
                };
                let slice = &data[start..start + len];
                let total = data.len();
                let etag = format!("\"{:x}\"", Sha256::digest(&data));
                write_response(
                    stream,
                    "206 Partial Content",
                    &[
                        (
                            "Content-Range",
                            &format!("bytes {start}-{end_inclusive}/{total}"),
                        ),
                        ("Content-Length", &len.to_string()),
                        ("ETag", &etag),
                        ("Content-Type", "application/octet-stream"),
                    ],
                    slice,
                );
            }
        });
        let r2_port = r2.addr.rsplit(':').next().unwrap().parse::<u16>().unwrap();
        let r2_base = r2.addr.clone();

        let report_api_404_flag = report_api_404;
        let api = MiniServer::spawn({
            let pending_store = pending_store.clone();
            let receipt_posts2 = receipt_posts2.clone();
            move |req, body, stream| {
                if req.method == "GET" && req.path.starts_with(PENDING_PATH) {
                    assert!(req.auth.as_deref().is_some_and(|a| a.contains("sdk_test")));
                    let body = pending_store.lock().unwrap().to_string();
                    write_response(
                        stream,
                        "200 OK",
                        &[("Content-Type", "application/json")],
                        body.as_bytes(),
                    );
                    return;
                }
                if req.method == "GET"
                    && req.path.contains("/stems/")
                    && req.path.contains("/deliveries/")
                {
                    assert!(req.auth.is_some());
                    assert!(req.range.is_some());
                    let loc = format!("{}/r2/stem?sig=1", r2_base);
                    write_response(stream, "307 Temporary Redirect", &[("Location", &loc)], b"");
                    return;
                }
                if req.method == "GET" && req.path.contains("/recovery-report") {
                    assert!(req.auth.is_some());
                    assert!(req.range.is_some());
                    if report_api_404_flag {
                        write_response(stream, "404 Not Found", &[], b"");
                        return;
                    }
                    let loc = format!("{}/r2/report?sig=2", r2_base);
                    write_response(stream, "307 Temporary Redirect", &[("Location", &loc)], b"");
                    return;
                }
                if req.method == "POST" && req.path.ends_with("/receipt") {
                    assert!(req.auth.is_some());
                    assert!(!body.is_empty());
                    *receipt_posts2.lock().unwrap() += 1;
                    write_response(
                        stream,
                        "200 OK",
                        &[("Content-Type", "application/json")],
                        br#"{"ok":true}"#,
                    );
                    return;
                }
                write_response(stream, "404 Not Found", &[], b"");
            }
        });

        Self {
            api,
            r2_port,
            receipt_posts,
            pending,
            report_r2_403_once,
        }
    }

    fn client(&self) -> HttpVoiceV2Client {
        HttpVoiceV2Client::new(&self.api.addr, "sdk_test_token_placeholder")
            .with_stem_redirect_allowlist(vec![format!("127.0.0.1:{}", self.r2_port)])
            .with_test_http_stem_redirects(true)
            .with_presign_retry_backoff(Duration::from_millis(5))
    }
}

#[test]
fn http_orchestrator_publishes_wav_and_report_then_receipts() {
    let tmp = tempfile::tempdir().unwrap();
    let wav = MINIMAL_WAV.to_vec();
    let report = b"Syndicate recovery report body\n".to_vec();
    let harness = SyndicateHarness::new(wav, report.clone());
    let client = harness.client();
    let pending = harness.pending.clone();
    let orch = FinalizedVoiceDelivery::open_recording_parent(tmp.path())
        .unwrap()
        .with_device_id("http-e2e-device");
    let mut phase = DeliveryPhase::Waiting;
    orch.deliver_pending(&client, &pending, &mut phase).unwrap();
    assert_eq!(phase, DeliveryPhase::Completed);
    assert_eq!(*harness.receipt_posts.lock().unwrap(), 1);
    let guild = pending.manifest.guild_id.clone();
    let sessions: Vec<_> =
        std::fs::read_dir(tmp.path().join("syndicate-discord-voice").join(guild))
            .unwrap()
            .flatten()
            .filter(|e| e.path().is_dir())
            .collect();
    assert_eq!(sessions.len(), 1);
    let session_dir = sessions[0].path();
    assert!(session_dir.join("user1.wav").is_file());
    assert_eq!(
        std::fs::read(session_dir.join(RECOVERY_REPORT_RELATIVE_PATH)).unwrap(),
        report
    );
}

#[test]
fn http_orchestrator_missing_report_endpoint_blocks_receipt() {
    let tmp = tempfile::tempdir().unwrap();
    let wav = MINIMAL_WAV.to_vec();
    let report = b"never fetched\n".to_vec();
    let harness = SyndicateHarness::with_options(wav, report, true);
    let client = harness.client();
    let pending = harness.pending.clone();
    let orch = FinalizedVoiceDelivery::open_recording_parent(tmp.path())
        .unwrap()
        .with_device_id("http-e2e-device");
    let mut phase = DeliveryPhase::Waiting;
    let err = orch
        .deliver_pending(&client, &pending, &mut phase)
        .unwrap_err();
    assert!(matches!(
        err,
        OrchestratorError::ClientTerminal(VoiceV2ClientError::RecoveryReportNotFound)
    ));
    assert_eq!(*harness.receipt_posts.lock().unwrap(), 0);
}

#[test]
fn http_orchestrator_corrupt_report_hash_blocks_receipt() {
    let tmp = tempfile::tempdir().unwrap();
    let wav = MINIMAL_WAV.to_vec();
    let good = b"good\n";
    let bad = b"bad!\n";
    let harness = SyndicateHarness::new(wav, bad.to_vec());
    let client = harness.client();
    let raw = json!({
        "version": 1,
        "sessionId": harness.pending.session_id,
        "guildId": "123456789012345678",
        "channelId": "234567890123456789",
        "sessionStartWallMs": 1,
        "sessionStartMonotonicNs": "100",
        "sealedStopWallMs": 2,
        "sealedStopMonotonicNs": "200",
        "targetSampleCount48k": 0,
        "journalFormatVersion": 2,
        "journalSha256": "a".repeat(64),
        "stems": harness.pending.manifest.stems.iter().map(|s| json!({
            "discordUserId": s.discord_user_id,
            "pathNick": s.path_nick,
            "wavRelativePath": s.wav_relative_path,
            "wavBytes": s.wav_bytes,
            "sha256": s.sha256,
        })).collect::<Vec<_>>(),
        "recoveryReport": {
            "relativePath": RECOVERY_REPORT_RELATIVE_PATH,
            "byteLength": good.len(),
            "sha256": format!("{:x}", Sha256::digest(good)),
            "contentType": "text/plain",
        }
    });
    let manifest = parse_syndicate_finalized_manifest(&raw).unwrap();
    let pending = PendingDelivery {
        session_id: harness.pending.session_id.clone(),
        manifest_digest: compute_finalized_manifest_digest(&manifest),
        sealed_at: harness.pending.sealed_at.clone(),
        manifest,
    };
    let orch = FinalizedVoiceDelivery::open_recording_parent(tmp.path())
        .unwrap()
        .with_device_id("http-e2e-device");
    let mut phase = DeliveryPhase::Waiting;
    let err = orch
        .deliver_pending(&client, &pending, &mut phase)
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("hash")
            || msg.contains("mismatch")
            || msg.contains("digest")
            || msg.contains("recovery"),
        "{msg}"
    );
    assert_eq!(*harness.receipt_posts.lock().unwrap(), 0);
}

#[test]
fn http_orchestrator_report_r2_403_retries_then_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    let wav = MINIMAL_WAV.to_vec();
    let report = b"presign-retry-report\n".to_vec();
    let harness = SyndicateHarness::new(wav, report.clone());
    *harness.report_r2_403_once.lock().unwrap() = true;
    let client = harness.client();
    let pending = harness.pending.clone();
    let orch = FinalizedVoiceDelivery::open_recording_parent(tmp.path())
        .unwrap()
        .with_device_id("http-e2e-device");
    let mut phase = DeliveryPhase::Waiting;
    orch.deliver_pending(&client, &pending, &mut phase).unwrap();
    assert_eq!(phase, DeliveryPhase::Completed);
    assert_eq!(*harness.receipt_posts.lock().unwrap(), 1);
}
