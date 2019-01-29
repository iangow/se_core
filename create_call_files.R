#!/usr/bin/env Rscript

# Code to generate a list of files in the StreetEvents directory
# and post to PostgreSQL.

# Set up stuff ----
library(dplyr, warn.conflicts = FALSE)
library(DBI)

getSHA1 <- function(file_name) {
    library("digest")
    digest(file=file_name, algo="sha1")
}

# Get a list of files ----
streetevent.dir <- file.path(Sys.getenv("SE_DIR"))
Sys.setenv(TZ='GMT')

full_path <- list.files(streetevent.dir, pattern="*_T.xml", recursive = TRUE,
                        include.dirs=TRUE, full.names = TRUE)

file_list <-
    tibble(full_path) %>%
    mutate(mtime = as.POSIXct(file.mtime(full_path)),
                 file_path = gsub(paste0(streetevent.dir, "/"), "", full_path,
                                                    fixed = TRUE))

pg <- dbConnect(RPostgreSQL::PostgreSQL())
rs <- dbGetQuery(pg, "SET TIME ZONE 'GMT'")
new_table <- !dbExistsTable(pg, c("streetevents", "call_files"))

cat("Updating data on", Sys.getenv("PGHOST"), "\n")

if (!new_table) {

    rs <- dbWriteTable(pg, c("streetevents", "call_files_temp"),
                       file_list %>% select(file_path, mtime),
                       overwrite=TRUE, row.names=FALSE)

    rs <- dbGetQuery(pg, "
        CREATE INDEX ON streetevents.call_files_temp (file_path, mtime)")

    call_files_temp <- tbl(pg, sql("SELECT * FROM streetevents.call_files_temp"))
    call_files <- tbl(pg, sql("SELECT * FROM streetevents.call_files"))

    new_files <-
        call_files_temp %>%
        select(file_path, mtime) %>%
        anti_join(call_files, by = c("file_path", "mtime")) %>%
        collect()

    rs <- dbGetQuery(pg, "DROP TABLE IF EXISTS streetevents.call_files_temp")

    if (dim(new_files)[1]>0) {
        new_files_plus <-
            new_files %>%
            inner_join(file_list %>% select(-mtime))
    } else {
        new_files_plus <- tibble()
    }
} else {
    new_files_plus <- file_list
}

rs <- dbDisconnect(pg)

process_rows <- function(df) {
    file_info <-
        file.info(df$full_path) %>%
        as_tibble() %>%
        transmute(file_size = size,
                  ctime = as.POSIXct(ctime))

    new_files_plus2 <-
        bind_cols(df, file_info) %>%
        mutate(file_name = gsub("\\.xml", "", basename(file_path))) %>%
        rowwise() %>%
        mutate(sha1 = getSHA1(full_path)) %>%
        select(file_path, file_size, mtime, ctime, file_name, sha1) %>%
        ungroup() %>%
        as_tibble()

    pg <- dbConnect(RPostgreSQL::PostgreSQL())
    rs <- dbGetQuery(pg, "SET TIME ZONE 'GMT'")

    if (dbExistsTable(pg, c("streetevents", "call_files"))) {
        rs <- dbWriteTable(pg, c("streetevents", "call_files"),
                   new_files_plus2 %>% as.data.frame(),
                   append=TRUE, row.names=FALSE)
    } else {
        rs <- dbWriteTable(pg, c("streetevents", "call_files"),
                           new_files_plus2 %>% as.data.frame(), row.names=FALSE)
        rs <- dbExecute(pg, "ALTER TABLE call_files OWNER TO streetevents")
        rs <- dbExecute(pg, "GRANT SELECT ON call_files TO streetevents_access")
        rs <- dbGetQuery(pg, "
            SET maintenance_work_mem='2GB';
            CREATE INDEX ON streetevents.call_files (file_path)")
    }

    dbDisconnect(pg)
}

split_df <- function(df, n = 10) {
    nrow <- nrow(df)
    if (nrow <= n) return(list(df))
    r  <- rep(1:(n+1), each=nrow/n)[1:nrow]
    split(df, r)
}

# Process files ----
if (dim(new_files_plus)[1]>0) {

    new_file_dfs <- split_df(new_files_plus, n = 1000)

    rs <- lapply(new_file_dfs, process_rows)
}
