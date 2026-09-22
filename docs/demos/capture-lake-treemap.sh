#!/bin/sh
# Record the interactive lake page as a looping click-through GIF.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

chrome="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
test -x "$chrome" || {
    echo "Google Chrome is required to capture the lake treemap" >&2
    exit 1
}
command -v ffmpeg >/dev/null 2>&1 || {
    echo "missing required command: ffmpeg" >&2
    exit 1
}
command -v python3 >/dev/null 2>&1 || {
    echo "missing required command: python3" >&2
    exit 1
}

html=.docker-data/pqbench-lake.html
frames=.docker-data/lake-frames
mkdir -p "$frames"
target/debug/pqbench bytemass docs/demos/lake.json --d3 >"$html"

python3 - "$html" "$frames" <<'PY'
import sys
from pathlib import Path

html_path, frames_dir = Path(sys.argv[1]), Path(sys.argv[2])
source = html_path.read_text()
marker = "show([{kind: 'collection', node: tree}]);"
if marker not in source:
    raise SystemExit("lake HTML is missing the initial show() call")

steps = [
    ("00-commerce", marker),
    (
        "01-retail",
        "show([{kind:'collection',node:tree},{kind:'catalog',node:tree.catalogs[0]}]);",
    ),
    (
        "02-bronze",
        "show([{kind:'collection',node:tree},{kind:'catalog',node:tree.catalogs[0]},{kind:'schema',node:tree.catalogs[0].schemas[0]}]);",
    ),
    (
        "03-orders-raw",
        "show([{kind:'collection',node:tree},{kind:'catalog',node:tree.catalogs[0]},{kind:'schema',node:tree.catalogs[0].schemas[0]},{kind:'table',node:tree.catalogs[0].schemas[0].tables[0]}]);",
    ),
    (
        "04-gold",
        "show([{kind:'collection',node:tree},{kind:'catalog',node:tree.catalogs[0]},{kind:'schema',node:tree.catalogs[0].schemas[1]}]);",
    ),
    (
        "05-fact-reviews",
        "show([{kind:'collection',node:tree},{kind:'catalog',node:tree.catalogs[0]},{kind:'schema',node:tree.catalogs[0].schemas[1]},{kind:'table',node:tree.catalogs[0].schemas[1].tables[1]}]);",
    ),
]
for name, show in steps:
    (frames_dir / f"{name}.html").write_text(source.replace(marker, show, 1))
    print(name)
PY

python3 -m http.server 8766 --directory "$frames" >/tmp/pqbench-lake-frames.log 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
sleep 0.3

i=0
for page in 00-commerce 01-retail 02-bronze 03-orders-raw 04-gold 05-fact-reviews; do
    "$chrome" --headless=new --disable-gpu --hide-scrollbars \
        --screenshot="$frames/frame-$i.png" --window-size=1440,900 \
        --virtual-time-budget=5000 \
        "http://127.0.0.1:8766/${page}.html"
    i=$((i + 1))
done

ffmpeg -y -framerate 1/2 -i "$frames/frame-%d.png" \
    -vf "fps=2,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse" \
    docs/images/pqbench-lake-treemap.gif
