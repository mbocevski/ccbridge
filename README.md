# ccbridge

Claude Code hook aggregator for Linux. Bridges Claude Code session events to
BLE (claude-desktop-buddy protocol), swaync notifications, and HTTP webhooks.

## Install

```sh
cd ~/dev/ccbridge
makepkg -si
ccbridged setup   # register hooks + enable user service
```

## Documentation

Full docs in `docs/` (TODO: task 8a96ea65).
