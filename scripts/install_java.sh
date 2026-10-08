#!/usr/bin/env bash
# Install Eclipse Temurin JDK 17 into ~/.local/share/jdk-17 (no sudo, no Homebrew).
# Spark finds it via spark.java_home in config/pipeline.yaml; the system Java is left alone.
set -euo pipefail

DEST="${JDK_DEST:-$HOME/.local/share/jdk-17}"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  OS=mac;   ARCH=aarch64 ;;
  Darwin-x86_64) OS=mac;   ARCH=x64 ;;
  Linux-x86_64)  OS=linux; ARCH=x64 ;;
  Linux-aarch64) OS=linux; ARCH=aarch64 ;;
  *) echo "unsupported platform: $(uname -sm)" >&2; exit 1 ;;
esac

if [[ -x "$DEST/Contents/Home/bin/java" || -x "$DEST/bin/java" ]]; then
  echo "JDK already installed in $DEST"; exit 0
fi

API="https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=$ARCH&image_type=jdk&os=$OS"
read -r NAME URL SHA < <(curl -fsSL "$API" | python3 -I -c '
import json, sys
p = json.load(sys.stdin)[0]["binary"]["package"]
print(p["name"], p["link"], p["checksum"])')

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
echo "Downloading $NAME"
curl -fsSL -o "$TMP/$NAME" "$URL"
echo "$SHA  $TMP/$NAME" | shasum -a 256 -c -

mkdir -p "$TMP/x" "$(dirname "$DEST")"
tar -xzf "$TMP/$NAME" -C "$TMP/x"
mv "$TMP"/x/jdk-* "$DEST"
JAVA="$DEST/Contents/Home/bin/java"; [[ -x "$JAVA" ]] || JAVA="$DEST/bin/java"
"$JAVA" -version
