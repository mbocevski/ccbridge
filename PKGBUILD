# Maintainer: Marko Bocevski <marko.bocevski@gmail.com>
#
# Build a ccbridge-git package directly from the GitHub repo:
#
#     git clone https://github.com/mbocevski/ccbridge.git
#     cd ccbridge
#     makepkg -si
#     ccbridged setup
#
# Not yet on the AUR — a separate backlog task tracks AUR submission.
# The PKGBUILD lives in-repo so contributors and early adopters can
# install via the standard makepkg flow without waiting for AUR.
pkgname=ccbridge-git
# pkgver is computed dynamically by pkgver(); this static value is a placeholder.
pkgver=0
pkgrel=1
pkgdesc="Claude Code hook aggregator — surfaces approvals via freedesktop notifications and a control socket"
arch=('x86_64')
url="https://github.com/mbocevski/ccbridge"
license=('MIT')
depends=()
makedepends=('cargo' 'git')
provides=('ccbridge')
conflicts=('ccbridge')
install=ccbridge.install
source=("ccbridge::git+https://github.com/mbocevski/ccbridge.git")
sha256sums=('SKIP')

pkgver() {
    cd "$srcdir/ccbridge"
    local desc
    desc=$(git describe --long --tags 2>/dev/null) || true
    if [[ -n "$desc" ]]; then
        echo "$desc" | sed 's/^v//;s/\([^-]*-g\)/r\1/;s/-/./g'
    else
        printf "r%s.g%s" "$(git rev-list --count HEAD)" "$(git rev-parse --short HEAD)"
    fi
}

build() {
    cd "$srcdir/ccbridge"
    export RUSTUP_TOOLCHAIN=stable
    export CARGO_TARGET_DIR="$srcdir/target"
    cargo build --release --workspace --locked --features ble
}

package() {
    cd "$srcdir/ccbridge"

    install -Dm755 "$srcdir/target/release/ccbridged"      "$pkgdir/usr/bin/ccbridged"
    install -Dm755 "$srcdir/target/release/ccbridge-hook"   "$pkgdir/usr/bin/ccbridge-hook"

    install -Dm644 contrib/systemd/ccbridge.service \
        "$pkgdir/usr/lib/systemd/user/ccbridge.service"

    install -Dm644 LICENSE \
        "$pkgdir/usr/share/licenses/$pkgname/LICENSE"

    install -Dm644 README.md \
        "$pkgdir/usr/share/doc/$pkgname/README.md"

    install -Dm644 docs/example-config.toml \
        "$pkgdir/usr/share/doc/$pkgname/example-config.toml"

    install -Dm644 docs/control-protocol.md \
        "$pkgdir/usr/share/doc/$pkgname/control-protocol.md"
}
