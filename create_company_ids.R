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

rs <- dbExecute(pg, "DROP TABLE IF EXISTS company_ids")

company_ids <-
    calls_raw %>%
    select(file_name, last_update, company_id, cusip, sedol, isin) %>%
    filter(!is.na(company_id) | !is.na(cusip) | !is.na(sedol) | !is.na(isin)) %>%
    distinct() %>% 
    compute(name = "company_ids", temporary = FALSE)

rs <- dbExecute(pg, "ALTER TABLE company_ids ADD PRIMARY KEY (file_name, last_update)")

db_comment <- paste0("CREATED USING create_company_ids.R from ",
                     "GitHub iangow/se_core ON ", Sys.time())
rs <- dbExecute(pg, paste0("COMMENT ON TABLE company_ids IS '",
                      db_comment, "';"))

rs <- dbExecute(pg, "ALTER TABLE company_ids OWNER TO streetevents")
rs <- dbExecute(pg, "GRANT SELECT ON company_ids TO streetevents_access")

rs <- dbDisconnect(pg)
