#!/usr/bin/env bash

set -euo pipefail

here="$(dirname "$0")"
src_root="$(readlink -f "${here}/..")"
cd "${src_root}"

if [[ -n ${BASE_SHA:-} && -n ${PACKAGE:-} ]]; then
  echo "Only one of BASE_SHA or PACKAGE should be provided" >&2
  exit 1
fi

# cargo-semver-checks only supports library targets, so proc-macro crates are skipped.
is_proc_macro() {
  # toml exits non-zero when the key is missing, which is the common case
  [[ "$(toml get -r "$1" lib.proc-macro 2>/dev/null)" == "true" ]]
}

if [[ -n ${PACKAGE:-} ]]; then
  manifest="$(cargo metadata --format-version 1 --no-deps |
    jq -r --arg name "${PACKAGE}" '.packages[] | select(.name == $name) | .manifest_path')"
  if is_proc_macro "${manifest}"; then
    echo "${PACKAGE}: proc-macro crate, skipping semver check"
    exit 0
  fi
  cargo semver-checks --package "${PACKAGE}"
  exit 0
fi

if [[ -z ${BASE_SHA:-} ]]; then
  echo "Either BASE_SHA or PACKAGE should be provided"
  exit 1
fi

mapfile -t members < <(toml get Cargo.toml workspace.members | jq -r '.[]')
changed_manifests=()

for member in "${members[@]}"; do
  manifest="${member%/}/Cargo.toml"
  package="$(toml get -r "${manifest}" package.name)"
  current_version="$(toml get -r "${manifest}" package.version)"
  base_manifest="$(mktemp)"

  if git show "${BASE_SHA}:${manifest}" > "${base_manifest}" 2>/dev/null; then
    base_version="$(toml get -r "${base_manifest}" package.version)"
  else
    base_version=""
  fi
  rm -f "${base_manifest}"

  echo "${package}: ${base_version:-<new>} -> ${current_version}"
  if [[ -z "${base_version}" ]]; then
    continue
  fi

  if [[ "${base_version}" != "${current_version}" ]]; then
    if is_proc_macro "${manifest}"; then
      echo "${package}: proc-macro crate, skipping semver check"
      continue
    fi
    changed_manifests+=("${manifest}")
  fi
done

if [[ ${#changed_manifests[@]} -eq 0 ]]; then
  echo "No workspace member versions changed"
  exit 0
fi

for manifest in "${changed_manifests[@]}"; do
  cargo semver-checks --manifest-path "${manifest}"
done
