# ccbridge

ccbridge is a background daemon for Linux that hooks into Claude Code and
aggregates state across all running sessions. When a tool call needs your
approval, it surfaces a dismissable notification via swaync (or any
freedesktop-compatible notification daemon: mako, dunst, GNOME, KDE) with
Approve and Deny actions so you can decide without switching windows. A bidirectional
control socket lets any script or TUI read live session state — token counts,
running/waiting counts, current approval prompt — and send decisions back.

**v1 scope:** Arch Linux only (install via PKGBUILD), freedesktop notifications
(swaync is the canonical example, but mako, dunst, GNOME, and KDE all work) and
control socket. A BLE bridge to claude-desktop-buddy hardware is planned for
v2 — the control socket already speaks the buddy wire protocol, so a BLE
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
notification appears (via swaync, mako, dunst, GNOME, KDE, or any
freedesktop-compatible daemon) with the tool name and input hint. Click **Approve** to
allow it or **Deny** to block it. If you ignore the notification, the approval
timeout expires and Claude Code falls back to its own built-in TUI prompt.

If the daemon is not running — or crashes — Claude Code behaves exactly as if
ccbridge were not installed. The hook binary exits 0 with no output on any
error; daemon-down is never a Claude Code outage.

## Control socket

The control socket at `$XDG_RUNTIME_DIR/ccbridge/ctrl.sock` is a
newline-delimited JSON stream. Connect with socat for quick inspection:

```sh
socat - UNIX-CONNECT:$XDG_RUNTIME_DIR/ccbridge/ctrl.sock
```

On connect you receive a `hello` message and a full heartbeat snapshot, then
a stream of heartbeat updates. To subscribe explicitly:

```json
{"cmd": "subscribe", "topics": ["heartbeat", "turn"]}
```

To approve or deny a pending tool call:

```json
{"cmd": "permission", "id": "<tool_use_id>", "decision": "once"}
{"cmd": "permission", "id": "<tool_use_id>", "decision": "deny"}
```

Full protocol reference: [docs/control-protocol.md](docs/control-protocol.md)
(documentation task in progress).

## Configuration

ccbridge reads `$XDG_CONFIG_HOME/ccbridge/config.toml` (defaults to
`~/.config/ccbridge/config.toml`). The config loader is not yet implemented
(task `8564c3f5`); all values are hardcoded defaults for now. The file is
reserved for future knobs such as approval timeout and which emitters are
enabled.

## Troubleshooting

**Check daemon logs:**

```sh
journalctl --user -u ccbridge -f
```

**Hooks not firing?** Re-run setup:

```sh
ccbridged setup
```

Safe to run repeatedly: missing hooks are added; existing hook entries are not
modified.

**Daemon not starting?** Verify the package is installed and the service unit
is in place:

```sh
pacman -Ql ccbridge-git | grep ccbridge.service
systemctl --user status ccbridge
```

**Claude Code broken after installing ccbridge?** That should not happen — the
hook binary exits 0 silently on any error. If you suspect a regression, remove
the hook entries from `~/.claude/settings.json` (the `hooks` key) and file an
issue.

## License

MIT. See [LICENSE](LICENSE).

## Contributing

Open an issue or pull request. This is a personal project without a formal
contributor process; reasonable patches welcome.
