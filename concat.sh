#!/bin/bash

OUTPUT_FOLDER=$1
CONFIGS=${@: 2}
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
python3 $SCRIPT_DIR/concat_v2.py -c $CONFIGS -o $OUTPUT_FOLDER -m 1

parallel --colsep '\t' --jobs 400% sox {1} {2} {3} ::::+ *_concat_info.tsv
rm *_concat_info.tsv