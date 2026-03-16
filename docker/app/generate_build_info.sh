#!/bin/sh
# docker/app/generate_build_info.sh
# Arguments: APP_VERSION GIT_COMMIT OUTPUT_FILE

# Fallback values in case ARGs are missing
APP_VERSION="${APP_VERSION:-unknown}"
GIT_COMMIT="${GIT_COMMIT:-0000000}"
OUTPUT_FILE="/opt/kronicle/src/kronicle/_build.py"

# Generate build date (UTC ISO 8601)
BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Normalize version (strip refs/)
if echo "$APP_VERSION" | grep -q '^refs/tags/'; then
    VERSION=$(echo "$APP_VERSION" | sed 's|refs/tags/||')
elif echo "$APP_VERSION" | grep -q '^refs/heads/'; then
    VERSION=$(echo "$APP_VERSION" | sed 's|refs/heads/||')
else
    VERSION="$APP_VERSION"
fi

# Short commit SHA
SHORT_COMMIT=$(echo "$GIT_COMMIT" | cut -c1-7)

# Write _build.py
printf "__version__='%s'\n__commit__='%s'\n__build_date__='%s'\n" \
  "$VERSION" "$SHORT_COMMIT" "$BUILD_DATE" > "$OUTPUT_FILE"

echo "Generated $OUTPUT_FILE:"
cat "$OUTPUT_FILE"
