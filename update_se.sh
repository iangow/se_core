#!/usr/bin/env bash
rsync -avz iangow@45.113.235.201:~/uploads/ $SE_DIR
echo "importing call_files"
./create_call_files.R
echo "importing calls"
./import_calls.R
echo "importing speaker_data"
./import_speaker_data.R
