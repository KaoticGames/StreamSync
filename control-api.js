// Privileged overlay HTTP — routes through native Tauri proxy when available.
// Never exposes master capability or accepts absolute URLs.
(function (global) {
  function overlayBase() {
    if (global.STREAMSYNC_OVERLAY && global.STREAMSYNC_OVERLAY.baseUrl) {
      return String(global.STREAMSYNC_OVERLAY.baseUrl).replace(/\/$/, "");
    }
    if (global.streamSyncOverlay && global.streamSyncOverlay.baseUrl) {
      return String(global.streamSyncOverlay.baseUrl).replace(/\/$/, "");
    }
    if (global.location && /^https?:/.test(global.location.protocol)) {
      return global.location.origin;
    }
    return "http://127.0.0.1:4040";
  }

  function getInvoke() {
    const tauri = global.__TAURI__;
    if (!tauri) return null;
    if (tauri.core && typeof tauri.core.invoke === "function") {
      return tauri.core.invoke.bind(tauri.core);
    }
    if (typeof tauri.invoke === "function") {
      return tauri.invoke.bind(tauri);
    }
    return null;
  }

  const standaloneReadOnly = !getInvoke();

  function validateRelativeApiPath(path) {
    const p = String(path || "").trim();
    if (!p || !p.startsWith("/api/")) {
      throw new Error("invalid_api_path");
    }
    if (/^[a-z]+:/i.test(p) || p.startsWith("//")) {
      throw new Error("absolute_url_forbidden");
    }
    if (p.includes("#") || p.includes("@") || p.includes("\\")) {
      throw new Error("unsafe_path");
    }
    const lower = p.toLowerCase();
    if (lower.includes("..") || lower.includes("%2f%2f") || lower.includes("%5c")) {
      throw new Error("traversal_forbidden");
    }
    return p;
  }

  async function readFormDataUpload(formData) {
    const profileId = formData.get("profile") || formData.get("profileId") || "default";
    const file = formData.get("file");
    if (!file || typeof file.arrayBuffer !== "function") {
      throw new Error("missing_upload_file");
    }
    const buffer = await file.arrayBuffer();
    const bytes = new Uint8Array(buffer);
    let binary = "";
    for (let i = 0; i < bytes.length; i += 1) {
      binary += String.fromCharCode(bytes[i]);
    }
    return {
      profile_id: String(profileId),
      file_name: file.name || "asset.bin",
      content_type: file.type || "application/octet-stream",
      data_base64: btoa(binary),
    };
  }

  async function privilegedFetch(path, options) {
    const safePath = validateRelativeApiPath(path);
    const opts = Object.assign({}, options || {});
    const method = (opts.method || "GET").toUpperCase();
    const invoke = getInvoke();

    if (invoke) {
      if (typeof FormData !== "undefined" && opts.body instanceof FormData) {
        if (safePath === "/api/events/upload-media" && method === "POST") {
          const upload = await readFormDataUpload(opts.body);
          const result = await invoke("overlay_media_upload", { request: upload });
          const status = Number(result && result.status) || 0;
          const text = (result && result.body) || "";
          const res = {
            ok: status >= 200 && status < 300,
            status,
            async text() {
              return text;
            },
            async json() {
              return JSON.parse(text || "null");
            },
          };
          if (status === 401) {
            const err = new Error("unauthorized");
            err.code = "unauthorized";
            err.status = 401;
            throw err;
          }
          return res;
        }
        throw new Error("multipart_requires_native_handling");
      }
      let body = undefined;
      let body_base64 = false;
      if (opts.body != null) {
        if (typeof opts.body === "string") {
          body = opts.body;
        } else {
          body = JSON.stringify(opts.body);
        }
      }
      const result = await invoke("overlay_api_request", {
        request: { method, path: safePath, body, body_base64 },
      });
      const status = Number(result && result.status) || 0;
      const text = (result && result.body) || "";
      const res = {
        ok: status >= 200 && status < 300,
        status,
        async text() {
          return text;
        },
        async json() {
          return JSON.parse(text || "null");
        },
      };
      if (status === 401) {
        const err = new Error("unauthorized");
        err.code = "unauthorized";
        err.status = 401;
        throw err;
      }
      return res;
    }

    // Privileged /api/* must not fall through to cookie-less fetch — that 401s
    // Import SE overlays (GET session/overlays) while Connections JWT save still
    // works (POST session is OAuthCompletion and can pass origin-only middleware).
    const err = new Error("Stream Sync control capability unavailable");
    err.code = "no_native_invoke";
    throw err;
  }

  global.streamSyncControlApi = {
    overlayBase,
    privilegedFetch,
    standaloneReadOnly,
    desktopRequiredMessage:
      "Saving and uploads require the Stream Sync desktop app. Read-only preview remains available in the browser.",
  };
})(typeof window !== "undefined" ? window : globalThis);
