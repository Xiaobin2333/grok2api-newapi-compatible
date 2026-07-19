#!/bin/sh
set -eu

umask 077

if [ ! -f "${GROK2API_CONFIG_SOURCE}" ]; then
  echo "missing config: ${GROK2API_CONFIG_SOURCE}" >&2
  echo "mount config.yaml to /run/grok2api/config.yaml" >&2
  exit 1
fi

cp "${GROK2API_CONFIG_SOURCE}" /app/config.yaml
chown grok2api:grok2api /app/config.yaml
chmod 0600 /app/config.yaml

ensure_directory() {
  path="$1"
  if [ -L "${path}" ] || { [ -e "${path}" ] && [ ! -d "${path}" ]; }; then
    echo "unsafe data directory: ${path}" >&2
    exit 1
  fi
  mkdir -p "${path}"
}

ensure_regular_file() {
  path="$1"
  if [ -L "${path}" ] || { [ -e "${path}" ] && [ ! -f "${path}" ]; }; then
    echo "unsafe data file: ${path}" >&2
    exit 1
  fi
}

ensure_directory /app/data
ensure_directory /app/data/media
ensure_directory /app/data/media/images
ensure_directory /app/data/media/videos

unsafe_link="$(find /app/data -xdev -type l -print | head -n 1)"
if [ -n "${unsafe_link}" ]; then
  echo "unsafe symbolic link in data directory: ${unsafe_link}" >&2
  exit 1
fi

ownership_marker="/app/data/.grok2api-ownership-v1"
ensure_regular_file "${ownership_marker}"
for database_file in /app/data/backend.db /app/data/backend.db-shm /app/data/backend.db-wal; do
  ensure_regular_file "${database_file}"
done

if [ ! -f "${ownership_marker}" ]; then
  : > "${ownership_marker}"
fi

find /app/data -xdev \( -type d -o -type f \) \( ! -user grok2api -o ! -group grok2api \) \
  -exec chown grok2api:grok2api {} \; -exec chmod u+rwX {} \;
find /app/data -xdev -type d ! -perm -0700 -exec chmod u+rwx {} \;
find /app/data -xdev -type f ! -perm -0600 -exec chmod u+rw {} \;
chmod 0600 "${ownership_marker}"
chmod 0700 /app/data /app/data/media /app/data/media/images /app/data/media/videos

if ! su-exec grok2api:grok2api sh -c '
  set -eu
  for directory in /app/data /app/data/media /app/data/media/images /app/data/media/videos; do
    probe="$(mktemp -d "${directory}/.grok2api-write-test.XXXXXX")"
    rmdir "${probe}"
  done
'; then
  echo "media storage is not writable by grok2api" >&2
  exit 1
fi

exec su-exec grok2api:grok2api "$@"
