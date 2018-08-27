# StreetEvents data

The code here transforms XML files for conference calls supplied by Thomson Reuters into structured tables in a PostgreSQL database.

## 1. Requirements

To use this code you will need a few things.

1. A directory containing the `.xml` files.
2. A PostgreSQL database to point to.
    - Database should have a schema `streetevents` and a role `streetevents`. The following SQL does
      this:

```sql
CREATE SCHEMA streetevents;
CREATE ROLE streetevents;
CREATE ROLE streetevents_access;
```

3. The following environment variables set:
    - `PGHOST`: The host address of the PostgreSQL database server.
    - `PGDATABASE`: The name of the PostgreSQL database.
    - `SE_DIR`: The path to the directory containing the `.xml` files.
    - `PGUSER` (optional): The default is your log-in ID; if that's not correct, set this variable.
    - `PGPASSWORD` (optional): This is not the recommended way to set your password, but is one
      approach.
4. R and the following packages: `xml2`, `stringr`, `dplyr`, `parallel`, `RPostgreSQL`, `digest`

## 2. Processing core tables

1. Get files from server.

```
rsync -avz iangow@45.113.235.201:~/uploads/ $SE_DIR
```

2. Run basic code.

The following three code files need to be run in the following order:

- The file `create_call_files.R` extracts details about the files associated with each call (e.g., `mtime`) and puts it in `streetevents.call_files`.
- The file `import_calls.R` extracts call-level data (e.g., ticker, call time, call type) and puts it in `streetevents.calls`.
- The file `import_speaker_data.R` parse the speaker-level data from the XML call files and puts it
  in `streetevents.speaker_data`.

The script `update_se.sh` does both of the steps above.
