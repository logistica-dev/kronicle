#!/bin/sh
# docker/app/generate_build_info.sh
# Arguments: GIT_COMMIT OUTPUT_FILE
#
# The application version is sourced from pyproject.toml at runtime (see
# kronicle.deps.settings_ini.package_version) — it is intentionally NOT
# baked in here.

# Fallback values in case ARGs are missing
GIT_COMMIT="${GIT_COMMIT:-0000000}"
OUTPUT_FILE="/opt/kronicle/src/kronicle/_build.py"
BUILD_DATE="${BUILD_DATE:-$(date -u +"%Y-%m-%dT%H:%M:%SZ")}"

# Short commit SHA
SHORT_COMMIT=$(echo "$GIT_COMMIT" | cut -c1-7)

# Write _build.py
printf "__commit__='%s'\n__build_date__='%s'\n" \
  "$SHORT_COMMIT" "$BUILD_DATE" > "$OUTPUT_FILE"

echo "Generated $OUTPUT_FILE:"
cat "$OUTPUT_FILE"
