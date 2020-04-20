pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET search_path TO streetevents")

call_files <- tbl(pg, "call_files")
call_files_temp <- tbl(pg, "call_files_temp")

call_files_temp %>% count()
call_files

current_call_files <-
    call_files %>%
    semi_join(call_files_temp)

dbExecute(pg, "DROP TABLE IF EXISTS moved_call_files")

moved_call_files <-
    call_files %>%
    select(file_path, sha1) %>%
    semi_join(current_call_files, by="sha1") %>%
    anti_join(current_call_files, by="file_path") %>%
    compute(name = "moved_call_files")
    
moved_call_files %>% count()

query <- "DELETE FROM call_files WHERE file_path IN (SELECT file_path FROM moved_call_files)"

dbExecute(pg, query)
