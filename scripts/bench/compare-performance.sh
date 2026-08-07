#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 BEFORE_DIR AFTER_DIR" >&2
    exit 2
fi

BEFORE="$1"
AFTER="$2"
WARP="${WARP:-warp}"

[ -d "$BEFORE" ] || { echo "missing before directory: $BEFORE" >&2; exit 1; }
[ -d "$AFTER" ] || { echo "missing after directory: $AFTER" >&2; exit 1; }

find "$BEFORE" -mindepth 2 -maxdepth 2 -type f \( -name '*.json.zst' -o -name '*.csv.zst' \) -print | sort | while IFS= read -r before_file; do
    relative="${before_file#"$BEFORE"/}"
    after_file="$AFTER/$relative"
    [ -f "$after_file" ] || { echo "missing matching result: $after_file" >&2; exit 1; }
    echo "Comparing $relative"
    "$WARP" cmp --no-color "$before_file" "$after_file"
done
