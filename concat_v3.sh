#!/bin/bash

OUTPUT_FOLDER=$1
CONFIGS=${@: 2}
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
python3 $SCRIPT_DIR/concat_v3.py -c $CONFIGS -o $OUTPUT_FOLDER

cat *_concat_info.tsv | parallel --jobs 400% "echo {} | xargs sox"
rm *_concat_info.tsv