# Debian/Ubuntu packaging

How ccbridge ships `.deb` packages and runs an apt repo on GitHub Pages.

## Overview

```
git tag v0.2.0
        │
        ▼
  release.yml
        │
        ├─► build amd64 .deb (cargo-deb on ubuntu-24.04)
        ├─► build arm64 .deb (cargo-deb on ubuntu-24.04-arm)
        │
        ├─► softprops/action-gh-release
        │   └─► attach both .debs to GitHub Release v0.2.0
        │
        └─► reprepro + GPG-sign
            └─► push to gh-pages branch under /apt/
                └─► users `apt install ccbridge`
```

Pushes to `main` (without a tag) take the same path but go to the
`beta` apt channel and skip the GitHub Release step.

## Channels

| Channel | Trigger | Suite | Audience |
|---|---|---|---|
| stable | `v*` git tags | `stable` | Everyone — what `apt install ccbridge` gives you by default. |
| beta   | push to `main` | `beta` | Anyone explicitly opted in via `apt install -t beta`. |

Both channels share the same signing key and are served from the same
gh-pages branch.

## Signing key (one-time maintainer setup)

1. Generate a release-only GPG key:

   ```sh
   gpg --quick-generate-key "ccbridge releases <marko.bocevski@gmail.com>" rsa4096 sign 5y
   gpg --armor --export-secret-keys ccbridge\ releases > /tmp/ccbridge-private.asc
   gpg --armor --export ccbridge\ releases > public-key.asc
   ```

2. In the repo's GitHub Settings → Secrets → Actions, add:

   - `APT_GPG_PRIVATE_KEY` — paste the contents of `/tmp/ccbridge-private.asc`
   - `APT_GPG_PASSPHRASE` — the passphrase you set during `--quick-generate-key`

3. Securely destroy the local copy of the private key
   (`shred /tmp/ccbridge-private.asc`).  GitHub Actions is now the
   only place the signing key lives; if the secrets ever leak,
   revoke and rotate.

4. The first push to `main` (or first `v*` tag) will publish the
   public key to `https://<user>.github.io/ccbridge/apt/ccbridge.asc`.
   No further maintainer action needed for normal releases.

## What lives where

```
public/                       (root of the gh-pages branch)
├── index.html                small landing page
└── apt/
    ├── ccbridge.asc          signing key (public, ASCII-armoured)
    ├── conf/distributions    reprepro config (regenerated each run)
    ├── pool/                 .deb files
    └── dists/
        ├── stable/Release    signed metadata
        └── beta/Release      signed metadata
```

reprepro is regenerating the metadata each run, but `pool/` is
preserved across runs (gh-pages branch keeps history) so previously-
published .debs stay available — users can pin to an exact version.

## Local verification before tagging

```sh
cargo install cargo-deb
cargo build --release --workspace
cargo deb --no-build --no-strip -p ccbridged
ls -lh target/debian/
dpkg-deb -I target/debian/*.deb     # show package metadata
dpkg-deb -c target/debian/*.deb     # show file contents
```

Install on a Debian/Ubuntu VM and confirm:

- `which ccbridged ccbridge-hook` finds them
- `systemctl --user list-unit-files ccbridge.service` shows the unit
- `dpkg -L ccbridge` shows the expected file list

## Troubleshooting

**`apt update` says "NO_PUBKEY"** — the user fetched the apt source
but skipped the key install.  Re-run the `curl` step from the
README's install instructions.

**`apt install ccbridge` 404s on a `.deb` file** — happens if a force-
push to gh-pages truncated old `pool/` content.  Re-run the release
workflow with `workflow_dispatch` to repopulate.

**arm64 user gets the amd64 package** — they didn't include
`[arch=arm64]` in their sources.list line.  The README snippet uses
`$(dpkg --print-architecture)` which auto-detects.
