# ccbridge

ccbridge is a background daemon for Linux that hooks into Claude Code and
aggregates state across all running sessions. When a tool call needs your
approval, it surfaces a dismissable notification via swaync (or any
freedesktop-compatible notification daemon: mako, dunst, GNOME, KDE) with
Approve / Deny / Always actions so you can decide without switching windows.
A bidirectional control socket lets any script or TUI read live session
state — token counts, running/waiting counts, current approval prompt — and
send decisions back.

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

**Edited a settings file and the allowlist didn't update?** ccbridge watches
`~/.claude/settings.json` and the per-project files (`<project>/.claude/settings.json`
and `settings.local.json`) and reloads the allowlist on change. Reload should
happen within ~100 ms of saving. Check the logs:

```sh
journalctl --user -u ccbridge | grep "reloaded allowlist"
```

If the line never appears, restart the daemon manually:
`systemctl --user restart ccbridge`.

**Claude Code broken after installing ccbridge?** That should not happen — the
hook binary exits 0 silently on any error. If you suspect a regression, remove
the `hooks` key from `~/.claude/settings.json` and file an issue.

## License

MIT. See [LICENSE](LICENSE).

## Contributing

Open an issue or pull request. This is a personal project without a formal
contributor process; reasonable patches welcome.
