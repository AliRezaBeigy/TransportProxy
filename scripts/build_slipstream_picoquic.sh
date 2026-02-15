#!/usr/bin/env bash
# Build slipstream-picoquic (submodule) with CMake. Same approach as slipstream-rust.
# Usage: run from repo root, or set PICOQUIC_DIR / PICOQUIC_BUILD_DIR.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PICOQUIC_DIR="${PICOQUIC_DIR:-"${ROOT_DIR}/slipstream-picoquic"}"
BUILD_DIR="${PICOQUIC_BUILD_DIR:-"${ROOT_DIR}/.slipstream-picoquic-build"}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
FETCH_PTLS="${PICOQUIC_FETCH_PTLS:-ON}"

if [[ ! -d "${PICOQUIC_DIR}" ]]; then
  echo "slipstream-picoquic not found at ${PICOQUIC_DIR}. Run: git submodule update --init --recursive" >&2
  exit 1
fi

IS_WINDOWS=0
case "${OSTYPE:-}" in
  msys*|cygwin*) IS_WINDOWS=1 ;;
esac
if [[ "$IS_WINDOWS" == "0" ]]; then
  UNAME_S=$(uname -s 2>/dev/null || echo "")
  case "$UNAME_S" in
    MSYS*|MINGW*|CYGWIN*) IS_WINDOWS=1 ;;
  esac
fi

CMAKE_ARGS=(
  "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
  "-DPICOQUIC_FETCH_PTLS=${FETCH_PTLS}"
  "-DCMAKE_POSITION_INDEPENDENT_CODE=ON"
  "-DBUILD_DEMO=OFF"
  "-DBUILD_HTTP=OFF"
  "-DBUILD_LOGLIB=OFF"
  "-DBUILD_LOGREADER=OFF"
  "-DBUILD_TESTING=OFF"
  "-Dpicoquic_BUILD_TESTS=OFF"
)

BUILD_TARGET=()

if [[ "$IS_WINDOWS" == "1" ]]; then
  if [[ -d "/c/Program Files/Microsoft Visual Studio/2022" ]] || [[ -d "C:/Program Files/Microsoft Visual Studio/2022" ]]; then
    CMAKE_ARGS+=("-G" "Visual Studio 17 2022" "-A" "x64")
    echo "Using Visual Studio 2022 generator" >&2
  elif [[ -d "/c/Program Files (x86)/Microsoft Visual Studio/2019" ]] || [[ -d "C:/Program Files (x86)/Microsoft Visual Studio/2019" ]]; then
    CMAKE_ARGS+=("-G" "Visual Studio 16 2019" "-A" "x64")
    echo "Using Visual Studio 2019 generator" >&2
  fi
  BUILD_TARGET=(--target picoquic-core picotls-core picotls-fusion picotls-minicrypto picotls-openssl)
fi

if [[ -n "${OPENSSL_ROOT_DIR:-}" ]]; then
  CMAKE_ARGS+=("-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}")
fi
if [[ -n "${OPENSSL_INCLUDE_DIR:-}" ]]; then
  CMAKE_ARGS+=("-DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}")
fi

cmake -S "${PICOQUIC_DIR}" -B "${BUILD_DIR}" "${CMAKE_ARGS[@]}"

if [[ "$IS_WINDOWS" == "1" ]]; then
  if [[ ${#BUILD_TARGET[@]} -gt 0 ]]; then
    cmake --build "${BUILD_DIR}" --config "${BUILD_TYPE}" "${BUILD_TARGET[@]}"
  else
    cmake --build "${BUILD_DIR}" --config "${BUILD_TYPE}"
  fi
  # Copy .lib to .a so Rust linker finds them (same as slipstream-rust)
  for BUILD_CONFIG in Debug Release; do
    RELEASE_DIR="${BUILD_DIR}/${BUILD_CONFIG}"
    PTLS_RELEASE="${BUILD_DIR}/_deps/picotls-build/${BUILD_CONFIG}"
    [[ -d "$RELEASE_DIR" ]] || continue
    for lib in picoquic-core picotls-core picotls-fusion picotls-minicrypto picotls-openssl; do
      src_dir="$RELEASE_DIR"
      [[ "$lib" != "picoquic-core" ]] && src_dir="$PTLS_RELEASE"
      if [[ -f "$src_dir/${lib}.lib" ]]; then
        cp "$src_dir/${lib}.lib" "${BUILD_DIR}/lib${lib}.a" 2>/dev/null || true
        underscored=$(echo "$lib" | tr '-' '_')
        cp "$src_dir/${lib}.lib" "${BUILD_DIR}/lib${underscored}.a" 2>/dev/null || true
      fi
    done
  done
else
  if [[ ${#BUILD_TARGET[@]} -gt 0 ]]; then
    cmake --build "${BUILD_DIR}" "${BUILD_TARGET[@]}"
  else
    cmake --build "${BUILD_DIR}"
  fi
fi
