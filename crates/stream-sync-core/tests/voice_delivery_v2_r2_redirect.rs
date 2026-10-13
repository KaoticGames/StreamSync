//! Live two-server tests: Syndicate API 307 → private R2 206 stem fetch.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use stream_sync_core::voice_delivery::{
    validate_recovery_report_range_meta, validate_stem_range_meta, HttpVoiceV2Client,
    ReceiptRequestBody, VoiceV2Client, VoiceV2ClientError,
};

#[derive(Default, Clone)]
struct CapturedRequest {
    method: String,
    path: String,
    auth: Option<String>,
    range: Option<String>,
    control: Option<String>,
}

fn read_http_request(stream: &mut TcpStream) -> CapturedRequest {
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
        } else if let Some(v) = lower.strip_prefix("range:") {
            cap.range = Some(v.trim().into());
        } else if let Some(v) = lower.strip_prefix("x-stream-sync-control-token:") {
            cap.control = Some(v.trim().into());
        } else if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(buf.len());
    let mut body_read = buf.len().saturating_sub(header_end);
    while body_read < content_length {
        let n = stream.read(&mut tmp).expect("read body");
        if n == 0 {
            break;
        }
        body_read += n;
    }
    cap
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

struct MiniServer {
    addr: String,
    _handle: thread::JoinHandle<()>,
}

impl MiniServer {
    fn spawn<F>(handler: F) -> Self
    where
        F: Fn(CapturedRequest, &mut TcpStream) + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(false).unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut stream = stream;
                let req = read_http_request(&mut stream);
                handler(req, &mut stream);
            }
        });
        Self {
            addr,
            _handle: handle,
        }
    }
}

fn test_client(api_base: &str, r2_port: u16) -> HttpVoiceV2Client {
    HttpVoiceV2Client::new(api_base, "sdk_test_token_placeholder")
        .with_stem_redirect_allowlist(vec![format!("127.0.0.1:{r2_port}")])
        .with_test_http_stem_redirects(true)
        .with_presign_retry_backoff(Duration::from_millis(10))
}

#[test]
fn api_307_r2_206_streams_range_without_bearer() {
    let stem_bytes: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(b"0123456789abcdef".to_vec()));
    let api_hits = Arc::new(Mutex::new(0u32));
    let r2_hits = Arc::new(Mutex::new(0u32));

    let r2_bytes = stem_bytes.clone();
    let r2_hits2 = r2_hits.clone();
    let r2 = MiniServer::spawn(move |req, stream| {
        *r2_hits2.lock().unwrap() += 1;
        assert_eq!(req.method, "GET");
        assert!(req.auth.is_none(), "R2 must not receive Authorization");
        assert!(req.control.is_none());
        assert_eq!(req.range.as_deref(), Some("bytes=0-7"));
        let data = r2_bytes.lock().unwrap();
        let slice = &data[0..8];
        write_response(
            stream,
            "206 Partial Content",
            &[
                ("Content-Range", "bytes 0-7/16"),
                ("Content-Length", "8"),
                ("ETag", "\"multipart-abc-2\""),
            ],
            slice,
        );
    });
    let r2_port = r2.addr.rsplit(':').next().unwrap().parse::<u16>().unwrap();
    let r2_url = format!("http://127.0.0.1:{r2_port}/obj?X-Amz-Signature=secret");

    let api_hits2 = api_hits.clone();
    let api = MiniServer::spawn(move |req, stream| {
        *api_hits2.lock().unwrap() += 1;
        assert_eq!(req.method, "GET");
        assert!(req.auth.as_deref().is_some_and(|a| a.contains("sdk_test")));
        assert_eq!(req.range.as_deref(), Some("bytes=0-7"));
        write_response(
            stream,
            "307 Temporary Redirect",
            &[("Location", &r2_url)],
            b"",
        );
    });

    let client = test_client(&api.addr, r2_port);
    let mut out = Vec::new();
    let meta = client
        .fetch_stem_range(
            "550e8400-e29b-41d4-a716-446655440000",
            "user1",
            0,
            8,
            &mut out,
        )
        .expect("stem fetch");
    assert_eq!(out, b"01234567");
    validate_stem_range_meta(&meta, 0, 16, 8).unwrap();
    assert_eq!(meta.etag, "\"multipart-abc-2\"");
    assert_eq!(*api_hits.lock().unwrap(), 1);
    assert_eq!(*r2_hits.lock().unwrap(), 1);
}

#[test]
fn recovery_report_api_307_r2_206_without_bearer() {
    let report_bytes: Arc<Mutex<Vec<u8>>> =
        Arc::new(Mutex::new(b"report-body-0123456789".to_vec()));
    let sha = {
        use sha2::{Digest, Sha256};
        format!(
            "{:x}",
            Sha256::digest(report_bytes.lock().unwrap().as_slice())
        )
    };
    let r2_bytes = report_bytes.clone();
    let r2 = MiniServer::spawn(move |req, stream| {
        assert_eq!(req.method, "GET");
        assert!(req.auth.is_none(), "R2 must not receive Authorization");
        assert_eq!(req.range.as_deref(), Some("bytes=0-7"));
        let data = r2_bytes.lock().unwrap();
        write_response(
            stream,
            "206 Partial Content",
            &[
                ("Content-Range", "bytes 0-7/20"),
                ("Content-Length", "8"),
                ("Content-Type", "text/plain; charset=utf-8"),
                ("ETag", "\"opaque-md5-or-multipart-2\""),
            ],
            &data[0..8],
        );
    });
    let r2_port = r2.addr.rsplit(':').next().unwrap().parse::<u16>().unwrap();
    let r2_url = format!("http://127.0.0.1:{r2_port}/recovery?sig=secret");
    let api = MiniServer::spawn(move |req, stream| {
        assert!(req.path.contains("/recovery-report"));
        assert!(req.auth.as_deref().is_some_and(|a| a.contains("sdk_test")));
        write_response(
            stream,
            "307 Temporary Redirect",
            &[("Location", &r2_url)],
            b"",
        );
    });
    let client = test_client(&api.addr, r2_port);
    let mut out = Vec::new();
    let meta = client
        .fetch_recovery_report_range("550e8400-e29b-41d4-a716-446655440000", 0, 8, &mut out)
        .expect("recovery report fetch");
    validate_recovery_report_range_meta(&meta, 0, 20, 8).unwrap();
    assert_ne!(meta.etag, format!("\"{sha}\""));
    assert_eq!(out, b"report-b");
}

#[test]
fn rejects_insecure_redirect_targets_before_r2_body() {
    let cases: Vec<(&str, &str)> = vec![
        ("https://evil.example/x", "host not allowed"),
        ("http://abc.r2.cloudflarestorage.com/x", "http downgrade"),
        (
            "https://user:pass@abc.r2.cloudflarestorage.com/x",
            "userinfo",
        ),
        (
            "https://abc.r2.cloudflarestorage.com:8443/x",
            "non-default port",
        ),
    ];
    for (location, _reason) in cases {
        let api = MiniServer::spawn(move |req, stream| {
            assert_eq!(req.range.as_deref(), Some("bytes=0-3"));
            write_response(
                stream,
                "307 Temporary Redirect",
                &[("Location", location)],
                b"",
            );
        });
        let client = HttpVoiceV2Client::new(&api.addr, "sdk_test");
        let mut out = Vec::new();
        let err = client
            .fetch_stem_range(
                "550e8400-e29b-41d4-a716-446655440000",
                "user1",
                0,
                4,
                &mut out,
            )
            .unwrap_err();
        assert!(
            matches!(err, VoiceV2ClientError::InsecureRedirect(_)),
            "location {location}: {err:?}"
        );
        assert!(out.is_empty());
    }
}

#[test]
fn rejects_missing_location_and_too_many_api_redirects() {
    let api = MiniServer::spawn(|req, stream| {
        assert_eq!(req.range.as_deref(), Some("bytes=0-1"));
        write_response(stream, "307 Temporary Redirect", &[], b"");
    });
    let client = HttpVoiceV2Client::new(&api.addr, "sdk_test");
    let mut out = Vec::new();
    assert!(matches!(
        client
            .fetch_stem_range(
                "550e8400-e29b-41d4-a716-446655440000",
                "user1",
                0,
                2,
                &mut out
            )
            .unwrap_err(),
        VoiceV2ClientError::InsecureRedirect(_)
    ));

    let r2 = MiniServer::spawn(|_, stream| {
        write_response(
            stream,
            "307 Temporary Redirect",
            &[("Location", "http://127.0.0.1:1/again")],
            b"",
        );
    });
    let r2_port = r2.addr.rsplit(':').next().unwrap().parse::<u16>().unwrap();
    let chain = r2.addr.clone();
    let api2 = MiniServer::spawn(move |_, stream| {
        write_response(
            stream,
            "307 Temporary Redirect",
            &[("Location", &chain)],
            b"",
        );
    });
    let client2 = test_client(&api2.addr, r2_port);
    let mut out2 = Vec::new();
    assert!(matches!(
        client2
            .fetch_stem_range(
                "550e8400-e29b-41d4-a716-446655440000",
                "user1",
                0,
                2,
                &mut out2
            )
            .unwrap_err(),
        VoiceV2ClientError::InsecureRedirect(_)
    ));
}

#[test]
fn r2_403_retries_through_api_with_fresh_url() {
    let api_calls = Arc::new(Mutex::new(0u32));
    let r2_calls = Arc::new(Mutex::new(0u32));
    let r2_calls2 = r2_calls.clone();
    let r2 = MiniServer::spawn(move |req, stream| {
        *r2_calls2.lock().unwrap() += 1;
        if req.path.contains("fresh") {
            write_response(
                stream,
                "206 Partial Content",
                &[
                    ("Content-Range", "bytes=0-3/8"),
                    ("Content-Length", "4"),
                    ("ETag", "\"opaque\""),
                ],
                b"data",
            );
        } else {
            write_response(stream, "403 Forbidden", &[], b"expired");
        }
    });
    let r2_port = r2.addr.rsplit(':').next().unwrap().parse::<u16>().unwrap();
    let stale = format!("{}/stale?sig=1", r2.addr);
    let fresh = format!("{}/fresh?sig=2", r2.addr);
    let api_calls2 = api_calls.clone();
    let api = MiniServer::spawn(move |_, stream| {
        let n = {
            let mut g = api_calls2.lock().unwrap();
            *g += 1;
            *g
        };
        let loc = if n == 1 {
            stale.as_str()
        } else {
            fresh.as_str()
        };
        write_response(stream, "307 Temporary Redirect", &[("Location", loc)], b"");
    });
    let client = test_client(&api.addr, r2_port);
    let mut out = Vec::new();
    client
        .fetch_stem_range(
            "550e8400-e29b-41d4-a716-446655440000",
            "user1",
            0,
            4,
            &mut out,
        )
        .unwrap();
    assert_eq!(out, b"data");
    assert_eq!(*api_calls.lock().unwrap(), 2);
    assert_eq!(*r2_calls.lock().unwrap(), 2);
}

#[test]
fn pending_rejects_cross_host_redirect() {
    let api = MiniServer::spawn(|_, stream| {
        write_response(
            stream,
            "307 Temporary Redirect",
            &[("Location", "https://evil.example/capture")],
            b"",
        );
    });
    let client = HttpVoiceV2Client::new(&api.addr, "sdk_test");
    let err = client.fetch_pending(1).unwrap_err();
    assert!(matches!(err, VoiceV2ClientError::InsecureRedirect(_)));
}

#[test]
fn receipt_rejects_cross_host_redirect() {
    let api = MiniServer::spawn(|_, stream| {
        write_response(
            stream,
            "307 Temporary Redirect",
            &[("Location", "https://evil.example/capture")],
            b"",
        );
    });
    let client = HttpVoiceV2Client::new(&api.addr, "sdk_test");
    let err = client
        .post_receipt(
            "550e8400-e29b-41d4-a716-446655440000",
            &ReceiptRequestBody {
                manifest_digest: "a".repeat(64),
                device_id: "d".into(),
                local_receipt_id: "r".into(),
                local_publication_state: "published".into(),
            },
        )
        .unwrap_err();
    assert!(matches!(err, VoiceV2ClientError::InsecureRedirect(_)));
}

#[test]
fn presigned_redirect_network_error_omits_url_and_signing_material() {
    use stream_sync_core::voice_delivery::OrchestratorError;

    const TOPSECRET: &str = "TOPSECRET";
    const AKIA: &str = "AKIAEXAMPLEKEY";
    const SECTOK: &str = "sectok";

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_port = listener.local_addr().unwrap().port();
    drop(listener);

    let presigned = format!(
        "http://127.0.0.1:{dead_port}/obj?X-Amz-Signature={TOPSECRET}&X-Amz-Credential={AKIA}&token={SECTOK}"
    );
    let api = MiniServer::spawn(move |_, stream| {
        write_response(
            stream,
            "307 Temporary Redirect",
            &[("Location", &presigned)],
            b"",
        );
    });
    let client = test_client(&api.addr, dead_port);
    let mut out = Vec::new();
    let err = client
        .fetch_stem_range(
            "550e8400-e29b-41d4-a716-446655440000",
            "user1",
            0,
            4,
            &mut out,
        )
        .unwrap_err();

    let display = err.to_string();
    let debug = format!("{err:?}");
    for leaked in [
        TOPSECRET,
        AKIA,
        SECTOK,
        "X-Amz-Signature=",
        dead_port.to_string().as_str(),
    ] {
        assert!(
            !display.contains(leaked),
            "display leaked {leaked}: {display}"
        );
        assert!(!debug.contains(leaked), "debug leaked {leaked}: {debug}");
    }
    assert!(
        matches!(err, VoiceV2ClientError::Network { .. }),
        "expected network error, got {err:?}"
    );

    let orch = OrchestratorError::ClientRetryable(err);
    let orch_display = orch.to_string();
    let orch_debug = format!("{orch:?}");
    for leaked in [TOPSECRET, AKIA, SECTOK] {
        assert!(!orch_display.contains(leaked));
        assert!(!orch_debug.contains(leaked));
    }
    assert!(orch_display.contains("network"));
}

const MINIMAL_WAV: [u8; 44] = [
    82, 73, 70, 70, 36, 0, 0, 0, 87, 65, 86, 69, 102, 109, 116, 32, 16, 0, 0, 0, 1, 0, 2, 0, 128,
    187, 0, 0, 0, 238, 2, 0, 4, 0, 16, 0, 100, 97, 116, 97, 0, 0, 0, 0,
];

fn pending_stem_only(wav: &[u8]) -> stream_sync_core::voice_delivery::PendingDelivery {
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use stream_sync_core::voice_delivery::{
        compute_finalized_manifest_digest, parse_syndicate_finalized_manifest, PendingDelivery,
    };
    let stem_sha = format!("{:x}", Sha256::digest(wav));
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
            "wavBytes": wav.len(),
            "sha256": stem_sha,
        }],
    });
    let manifest = parse_syndicate_finalized_manifest(&raw).unwrap();
    let digest = compute_finalized_manifest_digest(&manifest);
    PendingDelivery {
        session_id: manifest.session_id.clone(),
        manifest_digest: digest,
        sealed_at: "2020-01-01T00:00:00.000Z".into(),
        manifest,
    }
}

#[test]
fn orchestrator_two_server_opaque_r2_etag_differs_from_sha_and_succeeds() {
    use serde_json::json;
    use stream_sync_core::voice_delivery::{DeliveryPhase, FinalizedVoiceDelivery, PENDING_PATH};
    let tmp = tempfile::tempdir().unwrap();
    let wav = MINIMAL_WAV.to_vec();
    let pending = pending_stem_only(&wav);
    let manifest_value = serde_json::to_value(&pending.manifest).unwrap();
    let pending_json = json!({
        "ok": true,
        "deliveries": [{
            "sessionId": pending.session_id,
            "manifestDigest": pending.manifest_digest,
            "sealedAt": pending.sealed_at,
            "manifest": manifest_value,
        }]
    });
    let receipt_posts = Arc::new(Mutex::new(0u32));
    let stem_store = Arc::new(Mutex::new(wav.clone()));
    let r2 = MiniServer::spawn({
        let stem_store = stem_store.clone();
        move |req, stream| {
            assert!(req.auth.is_none());
            let data = stem_store.lock().unwrap();
            let range = req
                .range
                .as_deref()
                .unwrap()
                .strip_prefix("bytes=")
                .unwrap();
            let (start, end) = range.split_once('-').unwrap();
            let start = start.parse::<usize>().unwrap();
            let end = end.parse::<usize>().unwrap();
            let slice = &data[start..=end];
            write_response(
                stream,
                "206 Partial Content",
                &[
                    (
                        "Content-Range",
                        &format!("bytes {start}-{end}/{}", data.len()),
                    ),
                    ("Content-Length", &slice.len().to_string()),
                    ("ETag", "\"opaque-md5-or-multipart-2\""),
                ],
                slice,
            );
        }
    });
    let r2_port = r2.addr.rsplit(':').next().unwrap().parse::<u16>().unwrap();
    let r2_base = r2.addr.clone();
    let receipt_posts2 = receipt_posts.clone();
    let pending_store = Arc::new(Mutex::new(pending_json));
    let api = MiniServer::spawn({
        let pending_store = pending_store.clone();
        move |req, stream| {
            if req.path.starts_with(PENDING_PATH) {
                let body = pending_store.lock().unwrap().to_string();
                write_response(
                    stream,
                    "200 OK",
                    &[("Content-Type", "application/json")],
                    body.as_bytes(),
                );
                return;
            }
            if req.path.contains("/stems/") {
                let loc = format!("{}/obj?sig=1", r2_base);
                write_response(stream, "307 Temporary Redirect", &[("Location", &loc)], b"");
                return;
            }
            if req.method == "POST" && req.path.ends_with("/receipt") {
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
    let client = HttpVoiceV2Client::new(&api.addr, "sdk_test")
        .with_stem_redirect_allowlist(vec![format!("127.0.0.1:{r2_port}")])
        .with_test_http_stem_redirects(true)
        .with_presign_retry_backoff(Duration::from_millis(5));
    let orch = FinalizedVoiceDelivery::open_recording_parent(tmp.path())
        .unwrap()
        .with_device_id("r2-opaque-etag");
    let mut phase = DeliveryPhase::Waiting;
    orch.deliver_pending(&client, &pending, &mut phase).unwrap();
    assert_eq!(phase, DeliveryPhase::Completed);
    assert_eq!(*receipt_posts.lock().unwrap(), 1);
}

#[test]
fn orchestrator_two_server_corrupt_r2_body_fails_without_receipt() {
    use serde_json::json;
    use stream_sync_core::voice_delivery::{
        DeliveryPhase, FinalizedVoiceDelivery, OrchestratorError, PENDING_PATH,
    };
    let tmp = tempfile::tempdir().unwrap();
    let wav = MINIMAL_WAV.to_vec();
    let pending = pending_stem_only(&wav);
    let manifest_value = serde_json::to_value(&pending.manifest).unwrap();
    let pending_json = json!({
        "ok": true,
        "deliveries": [{
            "sessionId": pending.session_id,
            "manifestDigest": pending.manifest_digest,
            "sealedAt": pending.sealed_at,
            "manifest": manifest_value,
        }]
    });
    let wav_len = wav.len();
    let r2 = MiniServer::spawn(move |req, stream| {
        let range = req
            .range
            .as_deref()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap();
        let (start, end) = range.split_once('-').unwrap();
        let start = start.parse::<usize>().unwrap();
        let end = end.parse::<usize>().unwrap();
        let len = end + 1 - start;
        write_response(
            stream,
            "206 Partial Content",
            &[
                ("Content-Range", &format!("bytes {start}-{end}/{wav_len}")),
                ("Content-Length", &len.to_string()),
                ("ETag", "\"opaque-md5-or-multipart-2\""),
            ],
            &vec![0u8; len],
        );
    });
    let r2_port = r2.addr.rsplit(':').next().unwrap().parse::<u16>().unwrap();
    let r2_base = r2.addr.clone();
    let receipt_posts = Arc::new(Mutex::new(0u32));
    let receipt_posts2 = receipt_posts.clone();
    let pending_store = Arc::new(Mutex::new(pending_json));
    let api = MiniServer::spawn({
        let pending_store = pending_store.clone();
        move |req, stream| {
            if req.path.starts_with(PENDING_PATH) {
                let body = pending_store.lock().unwrap().to_string();
                write_response(
                    stream,
                    "200 OK",
                    &[("Content-Type", "application/json")],
                    body.as_bytes(),
                );
                return;
            }
            if req.path.contains("/stems/") {
                let loc = format!("{}/obj?sig=1", r2_base);
                write_response(stream, "307 Temporary Redirect", &[("Location", &loc)], b"");
                return;
            }
            if req.method == "POST" && req.path.ends_with("/receipt") {
                *receipt_posts2.lock().unwrap() += 1;
            }
            write_response(stream, "404 Not Found", &[], b"");
        }
    });
    let client = HttpVoiceV2Client::new(&api.addr, "sdk_test")
        .with_stem_redirect_allowlist(vec![format!("127.0.0.1:{r2_port}")])
        .with_test_http_stem_redirects(true);
    let orch = FinalizedVoiceDelivery::open_recording_parent(tmp.path())
        .unwrap()
        .with_device_id("r2-corrupt");
    let mut phase = DeliveryPhase::Waiting;
    let err = orch
        .deliver_pending(&client, &pending, &mut phase)
        .unwrap_err();
    assert!(matches!(err, OrchestratorError::Other(_)));
    assert_eq!(*receipt_posts.lock().unwrap(), 0);
}
