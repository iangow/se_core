#!/usr/bin/env bash
if [ "$USER" == "igow" ]
then
  ID="iangow"
else
  ID=$USER
fi

rsync -avz $ID@45.113.235.201:/home/thomsonreuters/uploads/ $SE_DIR