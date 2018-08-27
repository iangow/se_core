# StreetEvents data

The code here transforms XML files for conference calls supplied by Thomson Reuters into structured tables in a PostgreSQL database.

## Requirements

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

## Processing core tables

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

## Processing additional tables

A number of other tables are created using code from this repository. These generally depend on the
three tables above.

- `streetevents.crsp_link` is created by code in `crsp_link.sql`. 
This uses tickers and call dates to match firms to PERMNOs, but with some data integrity checks and manual overrides.
- `streetevents.qa_pairs` is created by `create_qa_pairs.R`.
This table attempts to group distinguish questions from answers and group questions and answers.
Often a single question will prompt multiple responses (e.g., the CEO answers at one level and the CFO provides more detail).
