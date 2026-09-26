// Update modal wiring for Stream Sync desktop (Tauri).
(function (global) {
  let wiredController = null;

  function ensureModal() {
    let backdrop = document.getElementById("update-modal-backdrop");
    if (backdrop) return backdrop;

    backdrop = document.createElement("div");
    backdrop.id = "update-modal-backdrop";
    backdrop.className = "update-modal-backdrop";
    backdrop.innerHTML = `
      <div class="update-modal" role="dialog" aria-labelledby="update-modal-title">
        <div class="update-modal__header">
          <h2 id="update-modal-title" class="update-modal__title">Update available</h2>
        </div>
        <div class="update-modal__body">
          <p id="update-modal-version" class="update-modal__version"></p>
          <pre id="update-modal-notes" class="update-modal__notes"></pre>
          <p id="update-modal-progress" class="update-modal__progress" hidden></p>
        </div>
        <div class="update-modal__footer">
          <div class="update-modal__footer-actions">
            <button type="button" class="btn btn-secondary" data-update-action="later">Later</button>
            <button type="button" class="btn btn-secondary" data-update-action="notes">View release notes</button>
            <button type="button" class="btn btn-secondary" data-update-action="fallback">Open update page</button>
            <button type="button" class="btn btn-primary" data-update-action="install">Download and install</button>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(backdrop);
    return backdrop;
  }

  function formatManualResult(result) {
    if (!result || typeof result !== "object") {
      return "Unable to check for updates.";
    }
    if (result.status === "upToDate") {
      return "You are on the latest version.";
    }
    if (result.status === "failed") {
      return result.message || "Unable to check for updates.";
    }
    if (result.status === "updateAvailable") {
      const version = result.version || "a newer version";
      return `Update available: Stream Sync ${version}`;
    }
    return "Unable to check for updates.";
  }

  function wireUpdateModal(invoke, listen) {
    if (!invoke || !listen) return;

    if (wiredController) return wiredController;

    const backdrop = ensureModal();
    let current = null;
    let installing = false;

    function closeModal() {
      backdrop.classList.remove("is-open");
      current = null;
      installing = false;
      const progress = backdrop.querySelector("#update-modal-progress");
      if (progress) {
        progress.hidden = true;
        progress.textContent = "";
      }
    }

    function openModal(payload) {
      current = payload;
      const versionEl = backdrop.querySelector("#update-modal-version");
      const notesEl = backdrop.querySelector("#update-modal-notes");
      if (versionEl) {
        versionEl.textContent = `Stream Sync ${payload.version} is available.`;
      }
      if (notesEl) {
        notesEl.textContent = payload.notes || "Release notes are available on GitHub.";
      }
      backdrop.classList.add("is-open");
    }

    backdrop.addEventListener("click", (event) => {
      if (event.target === backdrop) {
        closeModal();
      }
    });

    backdrop.querySelectorAll("[data-update-action]").forEach((button) => {
      button.addEventListener("click", async () => {
        const action = button.getAttribute("data-update-action");
        if (!current || installing) return;

        if (action === "later") {
          await invoke("dismiss_update", { version: current.version });
          closeModal();
          return;
        }

        if (action === "notes") {
          const url =
            current.releaseUrl ||
            `https://github.com/KaoticGames/StreamSync/releases/tag/v${current.version}`;
          if (window.electronAPI?.openExternal) {
            await window.electronAPI.openExternal(url);
          }
          return;
        }

        if (action === "fallback") {
          await invoke("open_update_fallback_page");
          return;
        }

        if (action === "install") {
          installing = true;
          const progress = backdrop.querySelector("#update-modal-progress");
          if (progress) {
            progress.hidden = false;
            progress.textContent = "Downloading update…";
          }
          try {
            await invoke("begin_update_install", { version: current.version });
          } catch (err) {
            installing = false;
            if (progress) {
              progress.textContent =
                err?.message || String(err) || "Update verification failed.";
            }
            alert(
              "Update failed:\n" +
                (err?.message || String(err) || "Verification failed.")
            );
          }
        }
      });
    });

    const ready = Promise.all([
      Promise.resolve(
        listen("update-available", (event) => {
          if (event?.payload) {
            openModal(event.payload);
          }
        })
      ),
      Promise.resolve(
        listen("update-download-progress", (event) => {
          const progress = backdrop.querySelector("#update-modal-progress");
          if (!progress || !event?.payload) return;
          progress.hidden = false;
          const downloaded = event.payload.chunkLength || 0;
          const total = event.payload.contentLength;
          progress.textContent = total
            ? `Downloading update… ${downloaded} / ${total} bytes`
            : "Downloading update…";
        })
      ),
      Promise.resolve(
        listen("update-download-finished", () => {
          const progress = backdrop.querySelector("#update-modal-progress");
          if (progress) {
            progress.hidden = false;
            progress.textContent = "Installing update…";
          }
        })
      ),
    ]).then(() => undefined);

    wiredController = { openModal, closeModal, formatManualResult, ready };
    return wiredController;
  }

  global.StreamSyncUpdateModal = {
    ensureModal,
    wireUpdateModal,
    formatManualResult,
  };
})(typeof globalThis !== "undefined" ? globalThis : window);
