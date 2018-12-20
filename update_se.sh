#!/usr/bin/env bash
if [ "$USER" == "igow" ]
then
  ID="iangow"
else
  ID=$USER
fi

rsync -avz $ID@45.113.235.201:~/uploads/ $SE_DIR
echo "importing call_files"
./create_call_files.R
echo "importing calls"
./import_calls_raw.R
./create_calls.R
./create_company_ids.R
echo "importing speaker_data"
./import_speaker_data.R
