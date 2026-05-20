#!/bin/bash

BASE_DIR="$(realpath $(dirname $0))"
DATA_DIR="${BASE_DIR}/data"
FROM_YEAR=""
TO_YEAR=""
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--data-dir)
      DATA_DIR="$2"
      shift 2
      ;;
    -f|--from)
      FROM_YEAR="$2"
      shift 2
      ;;
    -t|--to)
      TO_YEAR="$2"
      shift 2
      ;;
    *)
      echo "Error: Unrecognized argument $1"
      exit 1
      ;;
  esac
done

if [[ -z "$FROM_YEAR" ]] || [[ -z "$TO_YEAR" ]]; then
  echo "Error: --from (-f) and --to (-t) are mandatory parameters."
  echo "Usage: $0 -f <start_year> -t <end_year> [-d <data_dir>]"
  exit 1
fi

mkdir -p "${DATA_DIR}"

for ((year=FROM_YEAR; year<=TO_YEAR; year++)); do
  for month in {01..12}; do
    echo "Downloading ${year}-${month}..."
    yellow_filename="yellow_tripdata_${year}-${month}.parquet"

    if ! curl --fail -s "${BASE_URL}/${yellow_filename}" -o "${DATA_DIR}/${yellow_filename}"; then
      echo "  Warning: Failed to download ${yellow_filename} (HTTP error)"
      exit 1
    fi

    sleep 10
  done
  sleep 60
done
