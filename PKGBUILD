# Maintainer: Marko Bocevski <marko.bocevski@gmail.com>
pkgname=ccbridge-git
# pkgver is computed dynamically by pkgver(); this static value is a placeholder.
pkgver=0
pkgrel=1
pkgdesc="Claude Code hook aggregator — bridges Claude Code sessions to BLE/swaync/HTTP emitters"
arch=('x86_64')
url="https://github.com/mbocevski/ccbridge"
license=('MIT')
depends=()
makedepends=('cargo' 'git')
provides=('ccbridge')
conflicts=('ccbridge')
install=ccbridge.install
source=("ccbridge::git+file://$HOME/dev/ccbridge")
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
}
