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

The three code files below need to be run in the following order:

- The file `create_call_files.R` extracts details about the files associated with each call (e.g., `mtime`) and puts it in `streetevents.call_files`.
- The file `import_calls.R` extracts call-level data (e.g., ticker, call time, call type) and puts it in `streetevents.calls`.
- The file `import_speaker_data.R` parse the speaker-level data from the XML call files and puts it
  in `streetevents.speaker_data`.

The script `update_se.sh` does both of the steps above.

## 3. The tables used 

- `calls:` Primary key is (file_name, last_update). This table contains metadata on the calls (e.g., event description, company name, city, start time).

- `company_ids:` Primary key is (file_name, last_update). This table contains data on company identifiers. Note that we only have these variables for data supplied through the UniMelb subscription. Because StreetEvents deletes data for companies that cease to exist, older data of these companies has been retained, but these files do not have company IDs (such as CUSIP, ISIN).

- `speaker_data:` Primary key is (file_name, last_update, speaker_number, context, section), where context is either "pres" (presentation) or "qa" (questions and answers) and section is an integer that is used to distinguish portions of the call where there are multiple presentation and Q&A portions of the call (with speaker_number starting from 1 in each new section).

