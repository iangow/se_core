#!/usr/bin/env bash

rsync -e "ssh -p 2020 igow@iangow.me" -avz $SE_DIR igow@iangow.me:/media/igow/2TB/se_data/ 

