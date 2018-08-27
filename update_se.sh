#!/usr/bin/env bash
rsync -avz iangow@45.113.235.201:~/uploads/ $SE_DIR
./create_call_files.R
./import_calls.R
./import_speaker_data.R
