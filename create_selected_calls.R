# This code removes the duplicated call records from two sources
library(dplyr, warn.conflicts = FALSE)
library(DBI)

pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET search_path TO streetevents")
calls <- tbl(pg, "calls")
calls_raw <- tbl(pg, "calls_raw")
call_files <- tbl(pg, "call_files")

latest_calls <-
    calls %>%
    group_by(file_name) %>%
    # Filter file_name with no valid information
    filter(!is.na(start_date)) %>%
    summarize(last_update = max(last_update, na.rm = TRUE)) %>%
    ungroup() %>%
    compute()

latest_files <-
    calls_raw %>% 
    inner_join(call_files, by = c("file_path", "sha1", "file_name")) %>%
    group_by(file_name, file_path) %>%
    filter(mtime == max(mtime, na.rm = TRUE)) %>%
    group_by(file_name, last_update) %>%
    filter(file_path == min(file_path, na.rm = TRUE)) %>%
    select(file_name, file_path, last_update) %>%
    ungroup() %>%
    compute()

rs <- dbExecute(pg, "DROP TABLE IF EXISTS selected_calls")

selected_calls <-
    latest_calls %>%
    inner_join(latest_files, by = c("file_name", "last_update")) %>%
    compute(name = "selected_calls", temporary = FALSE)

rs <- dbExecute(pg, "ALTER TABLE selected_calls OWNER TO streetevents")
rs <- dbExecute(pg, "GRANT SELECT ON TABLE selected_calls TO streetevents_access")
rs <- dbExecute(pg, "CREATE INDEX ON selected_calls (file_name)")

comment <- 'CREATED USING iangow/se_core/create_selected_calls.R'
sql <- paste0("COMMENT ON TABLE selected_calls IS '",
              comment, " ON ", Sys.time() , "'")
rs <- dbExecute(pg, sql)
dbDisconnect(pg)

