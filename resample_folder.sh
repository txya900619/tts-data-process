#!/bin/bash

# This script resamples all the wav files in a folder to 22050 Hz
# arg1: input folder
# arg2: output folder

# Example usage:
# ./resample_folder.sh /path/to/input/folder /path/to/output/folder

INPUT_FOLDER=$1
OUTPUT_FOLDER=$2 

find $INPUT_FOLDER -type d -print0 | parallel -0 mkdir -p "{= s:$INPUT_FOLDER:$OUTPUT_FOLDER:; =}"
 
find $INPUT_FOLDER -name '*.wav' -type f -print0 | parallel --jobs 400% -0 sox {} -r 24000 "{= s:$INPUT_FOLDER:$OUTPUT_FOLDER:; =}"