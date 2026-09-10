# Stream Sync installer smoke checklist

Run this on a Windows machine after installing a **rebuild** of the NSIS setup, not a git pull of an already-installed app.

Manual — not automated in CI. Check each box on the build you are about to ship.

- [ ] Installer launches Stream Sync once; a second launch does not attach to a foreign process on the overlay port
- [ ] Help → Open download page opens `https://syndicateai.net/update` (or the configured HTTPS page). It does **not** ask for an update secret
- [ ] Twitch Personal connect completes (PKCE). Identity and scopes come from `/oauth2/validate`
- [ ] Twitch takeover / Delegated connect still works. Personal and Delegated never run as two live identities
- [ ] Kick connect works for the live identity
- [ ] Chat overlay and events overlay load in OBS / browser source
- [ ] Chat dock and events dock load; private dock capability still fences the dock
- [ ] Closing the window hides to tray; Quit from tray exits
- [ ] Backup export zip has no tokens; restore of an old zip with `C:/...` log paths still succeeds
- [ ] Discord recording folder + `/connect` key still ingest local WAV (if that feature is on this build)
- [ ] Logs land in `%APPDATA%\Stream Sync\logs\stream-sync-YYYY-MM-DD.log`. Purge keeps today. Secrets in log lines show `[REDACTED]`
