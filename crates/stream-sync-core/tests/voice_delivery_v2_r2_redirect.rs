//! Live two-server tests: Syndicate API 307 → private R2 206 stem fetch.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use stream_sync_core::voice_delivery::{
    validate_stem_range_meta, HttpVoiceV2Client, ReceiptRequestBody, VoiceV2Client,
    VoiceV2ClientError,
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
