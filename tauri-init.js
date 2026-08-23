// Early hook: mark Tauri desktop so shell can detect IPC/remote availability.
(function () {
  if (window.__TAURI__) {
    window.__STREAMSYNC_TAURI__ = true;
  }
})();
