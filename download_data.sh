#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://public-pme.s3.eu-west-3.amazonaws.com/text_bench"
DEST_DIR="${HOME}/text_bench"

mkdir -p "${DEST_DIR}"

echo "Select the dataset size to download:"
echo "1) 1B rows"
echo "2) 10B rows"
echo "3) 50B rows"
read -r -p "Enter choice [1]: " choice

choice="${choice:-1}"

download_range() {
    local start="$1"
    local end="$2"

    wget \
        --continue \
        --timestamping \
        --progress=dot:giga \
        --directory-prefix "${DEST_DIR}" \
        --input-file <(
            seq -f "${BASE_URL}/part_%03g.parquet" "${start}" "${end}"
        )
}

case "${choice}" in
    1)
        echo "Downloading part_000.parquet"
        wget \
            --continue \
            --timestamping \
            --progress=dot:giga \
            --directory-prefix "${DEST_DIR}" \
            "${BASE_URL}/part_000.parquet"
        ;;
    2)
        echo "Downloading part_000.parquet … part_009.parquet"
        download_range 0 9
        ;;
    3)
        echo "Downloading part_000.parquet … part_049.parquet"
        download_range 0 49
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

echo "Done"