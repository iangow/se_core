#!/usr/bin/env Rscript
library(DBI)
library(dplyr, warn.conflicts = FALSE)
library(tidyr)

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET work_mem TO '3GB'") 

# Define PG data ----

# Project schema
rs <- dbExecute(pg, "SET search_path TO streetevents, public")

call_files <- tbl(pg, "call_files")
calls_raw <- tbl(pg, "calls_raw")

original_names <-
    calls_raw %>%
    inner_join(call_files, by = c("file_path", "sha1", "file_name")) %>%
    group_by(file_name, last_update) %>%
    filter(mtime == min(mtime, na.rm = TRUE)) %>%
    select(sha1, file_path, file_name, last_update, company_name, call_desc, event_title, city) %>%
    ungroup() %>%
    distinct() %>%
    compute()

rs <- dbExecute(pg, "DROP TABLE IF EXISTS calls")

calls <-
    calls_raw %>%
    semi_join(original_names, by=c("sha1", "file_path")) %>%
    select(-file_path, -sha1, -company_id, -cusip, -sedol,
           -isin) %>%
    filter(!is.na(last_update)) %>%
    distinct() %>% 
    compute(name = "calls", temporary = FALSE)

rs <- dbExecute(pg, "ALTER TABLE calls ADD PRIMARY KEY (file_name, last_update)")

db_comment <- paste0("CREATED USING create_calls.R from ",
                     "GitHub iangow/se_core ON ", Sys.time())
rs <- dbExecute(pg, paste0("COMMENT ON TABLE calls IS '",
                      db_comment, "';"))

rs <- dbExecute(pg, "ALTER TABLE calls OWNER TO streetevents")
rs <- dbExecute(pg, "GRANT SELECT ON calls TO streetevents_access")

rs <- dbDisconnect(pg)
