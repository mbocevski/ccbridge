# ccbridge

ccbridge is a background daemon for Linux that hooks into Claude Code and
aggregates state across all running sessions. When a tool call needs your
approval, it surfaces a dismissable notification via swaync (or any
freedesktop-compatible notification daemon: mako, dunst, GNOME, KDE) with
Approve and Deny actions so you can decide without switching windows. A
bidirectional control socket lets any script or TUI read live session state —
token counts, running/waiting counts, current approval prompt — and send
decisions back.

**v1 scope:** Arch Linux only (install via PKGBUILD), freedesktop notifications
and control socket. A BLE bridge to claude-desktop-buddy hardware is planned
for v2 — the control socket already speaks the buddy wire protocol, so a BLE
bridge can live as a separate process that connects to ctrl.sock.

## Install

```sh
cd ~/dev/ccbridge
makepkg -si
ccbridged setup
```

`ccbridged setup` is a one-shot, idempotent step that registers the
`ccbridge-hook` binary in `~/.claude/settings.json` for the seven Claude Code
hook events ccbridge listens to (PreToolUse, PostToolUse, UserPromptSubmit,
Notification, Stop, SessionStart, SessionEnd), and enables the
`ccbridge.service` systemd user unit. Re-running it when already configured
is safe.

## What you'll see

When Claude Code is about to run a tool that needs approval, a critical
notification appears with the tool name and input hint. Click **Approve** to
allow it or **Deny** to block it. If you ignore the notification, the
approval timeout expires and Claude Code falls back to its own built-in TUI
prompt (configurable — see `fallback` in Configuration below).

ccbridge respects `permissions.allow` and `permissions.deny` entries in
`~/.claude/settings.json`. Tool calls that confidently match an allow-list
pattern are auto-approved without a notification; those matching a deny-list
pattern are hard-denied. Ambiguous or unrecognised patterns are surfaced with
an annotation explaining which pattern triggered the intercept. See
[docs/permission-handling.md](docs/permission-handling.md) for the full
decision logic.

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

Full protocol reference: [docs/control-protocol.md](docs/control-protocol.md).

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

## Configuration

ccbridge reads `$XDG_CONFIG_HOME/ccbridge/config.toml`
(defaults to `~/.config/ccbridge/config.toml`).
See [docs/example-config.toml](docs/example-config.toml) for the full
reference with all knobs and their defaults. The most likely things to tune:

| Key | Default | What it does |
|-----|---------|--------------|
| `approvals.timeout_ms` | `30000` | ms to wait for a decision before falling back |
| `approvals.fallback` | `"passthrough"` | `"passthrough"`, `"deny"`, or `"allow"` |
| `emit.notify.enabled` | `true` | Enable freedesktop desktop notifications |
| `emit.http.enabled` | `false` | Enable HTTP `/status` endpoint (Waybar) |
| `emit.http.addr` | `"127.0.0.1:9876"` | Address for the HTTP endpoint |

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

Safe to run repeatedly: missing hooks are added; existing entries are not
modified.

**Daemon not starting?** Verify the package and unit are in place:

```sh
pacman -Ql ccbridge-git | grep ccbridge.service
systemctl --user status ccbridge
```

**Edited `settings.json` and the allowlist didn't update?** ccbridge watches
`~/.claude/settings.json` and reloads the allowlist on change. Reload should
happen within ~100ms of saving the file. Check the logs for the reload event:

```sh
journalctl --user -u ccbridge | grep "allowlist"
```

If the "reloaded allowlist" line never appears, restart the daemon manually:
`systemctl --user restart ccbridge`.

**Claude Code broken after installing ccbridge?** That should not happen — the
hook binary exits 0 silently on any error. If you suspect a regression, remove
the `hooks` key from `~/.claude/settings.json` and file an issue.

## License

MIT. See [LICENSE](LICENSE).

## Contributing

Open an issue or pull request. This is a personal project without a formal
contributor process; reasonable patches welcome.
