#!/usr/bin/env bash
echo "Importing call_files"
echo "$PGHOST"
Rscript --vanilla create_call_files.R
echo "Importing calls"
Rscript --vanilla import_calls_raw.R
Rscript --vanilla create_calls.R
Rscript --vanilla create_company_ids.R
Rscript --vanilla create_selected_calls.R
echo "Importing speaker_data"
Rscript --vanilla import_speaker_data.R
if [ "$USER" == "igow" ]
then
  ID="iangow"
else
  ID=$USER
fi

rsync -avz $ID@45.113.235.201:~/uploads/ $SE_DIR
echo "importing call_files"
Rscript --vanilla create_call_files.R
echo "importing calls"
Rscript --vanilla import_calls_raw.R
Rscript --vanilla create_calls.R
Rscript --vanilla create_company_ids.R
echo "importing speaker_data"
Rscript --vanilla import_speaker_data.R
