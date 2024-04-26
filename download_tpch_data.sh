#!/usr/bin/env bash

set -e

usage() {
    echo "Usage: $0 <scale_factor> <output_dir>"
    echo "Downloads TPCH tbl files at the specified scale factor into the specified directory."
    exit 1
}

if [ $# -ne 2 ]; then
    usage
fi

SCALE_FACTOR=$1
OUTPUT_DIR=$2

# Function to download TPCH data
download_tpch() {
    local scale_factor=$1
    local output_dir=$2

    echo "Creating TPCH dataset at Scale Factor ${scale_factor} in ${output_dir}..."

    # Ensure the target data directory exists
    mkdir -p "${output_dir}"

    # Create 'tbl' (CSV format) data into output directory if it does not already exist
    local file="${output_dir}/supplier.tbl"
    if test -f "${file}"; then
        echo "tbl files exist ($file exists)."
    else
        echo "Creating tbl files with tpch_dbgen..."
        # Use docker to generate TPCH data
        docker run -v "${output_dir}":/data -it --rm ghcr.io/scalytics/tpch-docker:main -vf -s ${scale_factor}
    fi
}

download_tpch "$SCALE_FACTOR" "$OUTPUT_DIR"
