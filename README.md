# ccbridge

[![CI](https://github.com/mbocevski/ccbridge/actions/workflows/ci.yml/badge.svg)](https://github.com/mbocevski/ccbridge/actions/workflows/ci.yml)

ccbridge is a background daemon for Linux that hooks into Claude Code and
aggregates state across all running sessions. When a tool call needs your
approval, it surfaces a dismissable notification through your freedesktop
notification daemon with Approve / Deny / Always actions so you can decide
without switching windows. A bidirectional control socket lets any script
or TUI read live session state — token counts, running/waiting counts,
current approval prompts — and send decisions back.

## Install (Arch Linux)

ccbridge ships an in-repo `PKGBUILD` that builds a `ccbridge-git`
package from the GitHub repository:

```sh
git clone https://github.com/mbocevski/ccbridge.git
cd ccbridge
makepkg -si
ccbridged setup
```

`makepkg -si` builds the package, prompts for the sudo password,
installs it system-wide, and enables ccbridge to be available as
`/usr/bin/ccbridged` and `/usr/bin/ccbridge-hook`.  It also drops
the `ccbridge.service` systemd user unit at
`/usr/lib/systemd/user/ccbridge.service`.

To upgrade later, pull the new commits and re-run `makepkg -si` —
the dynamic `pkgver()` function in the PKGBUILD picks up the new
version from `git describe`, so pacman recognises the upgrade.

`ccbridged setup` is a one-shot, idempotent step that:

1. Registers the `ccbridge-hook` binary in `~/.claude/settings.json` for the
   seven Claude Code hook events ccbridge listens to (PreToolUse, PostToolUse,
   UserPromptSubmit, Notification, Stop, SessionStart, SessionEnd).
2. Writes a default `~/.config/ccbridge/config.toml` if one doesn't already
   exist — never overwrites a user-edited file.
3. Enables the `ccbridge.service` systemd user unit.

Re-running when already configured is safe: the settings file is left
byte-for-byte unchanged when every hook is already registered, and the
config is left untouched whenever it exists.

## Install (Debian / Ubuntu)

Pre-built `.deb` packages are published from CI to a signed apt repo
hosted on GitHub Pages.  Two channels are available:

- **stable** — published on every `v*` git tag.
- **beta** — published on every push to `main`.

One-time setup (adds the signing key and the apt source):

```sh
# Trust the ccbridge release signing key.
sudo curl -fsSLo /etc/apt/keyrings/ccbridge.asc \
  https://mbocevski.github.io/ccbridge/apt/ccbridge.asc

# Add the apt source.  Use `stable` or `beta` as the suite name.
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/ccbridge.asc] \
  https://mbocevski.github.io/ccbridge/apt stable main" \
  | sudo tee /etc/apt/sources.list.d/ccbridge.list

sudo apt update
sudo apt install ccbridge
ccbridged setup
```

Per-user setup (`ccbridged setup`) is the same as on Arch — registers
hooks in `~/.claude/settings.json`, writes `~/.config/ccbridge/config.toml`
if absent, enables the `ccbridge.service` systemd user unit.

To switch from stable to beta later: change `stable` to `beta` in
`/etc/apt/sources.list.d/ccbridge.list`, run `sudo apt update`, and
the next `apt install --only-upgrade ccbridge` picks up beta builds.

To uninstall: `sudo apt remove ccbridge`.  Per-user state in
`~/.claude/` and `~/.config/ccbridge/` is left in place (dpkg doesn't
manage user data); remove it manually if you want a clean wipe.

## What you'll see

When Claude Code is about to run a tool that needs approval, a critical
notification appears with the tool name and input hint. Click **Approve** to
allow it, **Deny** to block it, or **Always** to also add a specific
allowlist entry so this exact operation auto-approves in the future.
ccbridge writes to `<project>/.claude/settings.local.json` (project-local,
gitignored by default) so approvals don't silently apply to every project
you work on. The project root is the nearest ancestor of `cwd` that has
`.claude/` or `.git`; if none is found, ccbridge bootstraps `cwd` itself
as a project (creating `cwd/.claude/` if it doesn't exist). ccbridge
**never** writes to your user-global `~/.claude/settings.json` — that
file is yours alone.
ccbridge picks the most-narrow pattern that matches (e.g. clicking Always
on `Bash(git status)` adds `Bash(git status)`, not `Bash`). For tools
where a specific pattern can't be auto-derived, ccbridge declines rather
than risk a too-broad allowlist.

If you click Always by mistake, run `ccbridged undo-last-allow` to remove
the most-recently added pattern and restore the previous settings.

If you ignore the notification, the approval timeout expires and Claude Code
falls back to its own built-in TUI prompt (configurable — see `fallback` in
Configuration below).

When Claude finishes a response and you don't immediately follow up,
ccbridge posts a low-key "Claude is done" notification (normal urgency,
auto-expires after 5s, no action buttons). It only fires after a
configurable idle window (10s by default) so a Stop emitted between tool
calls in a multi-step task doesn't trigger one. Turn it off with
`emit.notify.turn_done.enabled = false` in `config.toml`.

ccbridge respects `permissions.allow` and `permissions.deny` entries from
three files, cascaded in this order:

1. `<project>/.claude/settings.local.json` (project-local, gitignored) — where
   Always writes
2. `<project>/.claude/settings.json` (project-local, checked in)
3. `~/.claude/settings.json` (user-global, your own config)

Tool calls that confidently match an allow-list pattern are auto-approved
without a notification; those matching a deny-list pattern are hard-denied
(deny still wins overall when the same call matches both lists). Ambiguous
or unrecognised patterns are surfaced with an annotation in the notification
body explaining which pattern triggered the intercept. Each file is hot-reloaded
on change — edit any of them and the next tool call sees the new rules.

If the daemon is not running or crashes, Claude Code behaves exactly as if
ccbridge were not installed. The hook binary exits 0 with no output on any
error — daemon-down is never a Claude Code outage.

## Control socket

The control socket at `$XDG_RUNTIME_DIR/ccbridge/ctrl.sock` is a
newline-delimited JSON stream. Quick inspection:

```sh
socat - UNIX-CONNECT:$XDG_RUNTIME_DIR/ccbridge/ctrl.sock
```

On connect you receive a `hello` message and a full heartbeat snapshot, then
a stream of heartbeat updates (subscribe first to keep receiving them):

```json
{"cmd": "subscribe", "topics": ["heartbeat", "turn"]}
```

To approve or deny a pending tool call:

```json
{"cmd": "permission", "id": "<tool_use_id>", "decision": "once"}
{"cmd": "permission", "id": "<tool_use_id>", "decision": "deny"}
```

The wire format mirrors the BLE Nordic UART hardware-bridge protocol so any client that can speak newline-delimited JSON can subscribe. Protocol types live in `crates/ccbridge-proto/src/ctrl.rs` and `buddy.rs`.

## Waybar integration

Enable the optional HTTP endpoint in `~/.config/ccbridge/config.toml`:

```toml
[emit.http]
enabled = true
addr = "127.0.0.1:9876"
```

Then add a custom module to `~/.config/waybar/config`:

```jsonc
"custom/ccbridge": {
    "format": "{} 󱙯",
    "interval": 10,
    "exec": "curl -sf http://127.0.0.1:9876/status | jq -r '\"\\(.tokens_today) toks\"' 2>/dev/null || echo '-'",
    "tooltip": false
}
```

`GET /status` returns the full heartbeat JSON snapshot (same shape as the
ctrl-socket heartbeat). Only `GET /status` is served; everything else returns
404.

**Loopback-only:** ccbridge refuses to bind the HTTP endpoint to any non-loopback
address (`0.0.0.0`, LAN IPs, etc.). The heartbeat contains `cwd`, `session_id`,
`agent_type`, and tool command hints that must not be exposed to the network.
Only `127.0.0.1` (IPv4) and `::1` (IPv6) are accepted; a non-loopback `addr`
in the config produces a warning and disables the endpoint without crashing the
daemon.

## BLE bridge

ccbridge can mirror the heartbeat — and accept Approve/Deny decisions back —
over Bluetooth Low Energy. ccbridge plays the **central** role; any peripheral
that advertises the Nordic UART Service (NUS) and speaks ccbridge's JSON-on-NUS
dialect is supported. The reference firmware lives at
[ccbridge-buddy](https://github.com/mbocevski/ccbridge-buddy) (ESP32-S3).

**Pairing happens via the OS.** ccbridge consumes already-paired devices from
BlueZ; it never initiates pairing itself. Pair once with whatever tool you
prefer:

```sh
# Power on, scan, pair, trust — once per device.
bluetoothctl power on
bluetoothctl scan on        # find your device's MAC
bluetoothctl pair AA:BB:CC:DD:EE:FF
bluetoothctl trust AA:BB:CC:DD:EE:FF
bluetoothctl scan off
```

(Or use any GUI Bluetooth tool — blueman, GNOME Bluetooth, KDE Bluetooth.)

Then enable the bridge in `~/.config/ccbridge/config.toml`:

```toml
[emit.ble]
enabled = true
```

Restart the daemon. Every paired device that advertises the NUS service UUID
gets its own session — multiple devices work in parallel. The device receives
an `OwnerMessage` + `TimeSync` on connect, then a stream of `Heartbeat`
snapshots; pressing Approve / Deny on the device sends a `PermissionCmd` back
which the daemon routes into the same approval pipeline as desktop notifications.

To nickname or disable a specific paired device without un-pairing it:

```toml
[[emit.ble.device]]
address  = "AA:BB:CC:DD:EE:FF"
nickname = "desk buddy"
disabled = false
```

To stop using a device entirely, un-pair it via the OS
(`bluetoothctl remove AA:BB:CC:DD:EE:FF`) — ccbridge picks up the removal and
shuts down the session within a few seconds.

## Configuration

ccbridge reads `$XDG_CONFIG_HOME/ccbridge/config.toml`.  See
[docs/example-config.toml](docs/example-config.toml) for the full
reference.

| Key | Default | What it does |
|-----|---------|--------------|
| `approvals.timeout_ms` | `30000` | ms to wait for a decision before falling back |
| `approvals.fallback` | `"passthrough"` | `"passthrough"`, `"deny"`, or `"allow"` |
| `emit.notify.enabled` | `true` | Enable freedesktop desktop notifications |
| `emit.notify.turn_done.enabled` | `true` | Post "Claude is done" notification when a session has been idle after Stop |
| `emit.notify.turn_done.idle_grace_ms` | `10000` | How long a session must be idle after Stop before the notification fires |
| `emit.http.enabled` | `false` | Enable HTTP `/status` endpoint (Waybar) |
| `emit.http.addr` | `"127.0.0.1:9876"` | Address for the HTTP endpoint |
| `emit.ble.enabled` | `false` | Mirror heartbeat to paired BLE peripherals (NUS) |
| `emit.ble.service_uuid` | NUS UUID | Service UUID a paired device must advertise |

To apply a config change: edit the file and restart the daemon
(`systemctl --user restart ccbridge`). There is no hot-reload for
`config.toml`; only `settings.json` allowlist changes are picked up live.

## Troubleshooting

**Check daemon logs:**

```sh
journalctl --user -u ccbridge -f
```

**Hooks not firing?** Re-run setup:

```sh
ccbridged setup
```

**Daemon not starting?** Verify the package and unit are in place:

```sh
pacman -Ql ccbridge-git | grep ccbridge.service
systemctl --user status ccbridge
```

**Edited a settings file and the allowlist didn't update?** ccbridge watches
`~/.claude/settings.json` and the per-project files (`<project>/.claude/settings.json`
and `settings.local.json`) and reloads the allowlist on change. Reload should
happen within ~100 ms of saving. Check the logs:

```sh
journalctl --user -u ccbridge | grep "reloaded allowlist"
```

If the line never appears, restart the daemon manually:
`systemctl --user restart ccbridge`.

**Claude Code misbehaving after installing ccbridge?** The hook binary exits
0 silently on any error, so the daemon should never break Claude Code.  If
you suspect a regression, remove the `hooks` key from
`~/.claude/settings.json` and file an issue.

## License

MIT.  See [LICENSE](LICENSE).

## Contributing

Open an issue or pull request.
