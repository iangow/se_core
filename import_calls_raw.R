#!/usr/bin/env Rscript

library(RPostgreSQL)
library(dplyr, warn.conflicts = FALSE)
library(xml2)
library(parallel)
library(stringr)
se_path <- file.path(Sys.getenv("SE_DIR"))

getSHA1 <- function(file_name) {
    library("digest")
    digest(file=file_name, algo="sha1")
}

is.error <- function(x) inherits(x, "try-error")

extract_call_data <- function(file_path) {
    full_path <- file.path(se_path, file_path)
    sha1 <- getSHA1(full_path)
    if (!file.exists(full_path)) return(NULL)

    file_name <- str_replace(basename(file_path), "\\.xml$", "")
    read_data <- function(se_path, file_path) {
        file_xml <- read_xml(file.path(se_path, file_path), options = "NOENT")
        last_update <- as.POSIXct(xml_attr(file_xml, "lastUpdate"),
                              format="%A, %B %d, %Y at %H:%M:%S%p GMT", tz = "GMT")
        event_type = xml_attr(file_xml, "eventTypeId")
        event_type_name <- xml_attr(file_xml, "eventTypeName")
        call_desc <- xml_text(xml_child(file_xml, search = "/Headline"))
        city <- xml_text(xml_child(file_xml, search = "/city"))
        company_name <- trimws(xml_text(xml_child(file_xml, search = "/companyName")))
        company_ticker <- trimws(xml_text(xml_child(file_xml, search = "/companyTicker")))
        start_date <- as.POSIXct(xml_text(xml_child(file_xml, search = "/startDate")),
                             format="%d-%b-%y %H:%M%p GMT", tz = "GMT")
        company_id <- xml_text(xml_child(file_xml, search = "/companyId"))
        cusip <- xml_text(xml_child(file_xml, search = "/CUSIP"))
        sedol <- xml_text(xml_child(file_xml, search = "/SEDOL"))
        isin <- xml_text(xml_child(file_xml, search = "/ISIN"))
        event_title <- xml_text(xml_child(file_xml, search = "/eventTitle"))
        city <- xml_text(xml_child(file_xml, search = "/city"))


        tibble(file_path, sha1, file_name, last_update, company_name,
           company_ticker, start_date, company_id, cusip, sedol, isin,
           event_type, event_type_name, call_desc, event_title, city)
    }

    empty_df <- tibble(file_path, sha1, file_name)

    file_data <- try(read_data(se_path, file_path))
    if (is.error(file_data)) return(empty_df)
    return(file_data)
}

pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("streetevents", "calls_raw"))) {
    dbGetQuery(pg, "
        CREATE TABLE streetevents.calls_raw
            (
              file_path text,
              sha1 text,
              file_name text,
              last_update timestamp with time zone,
              company_name text,
              company_ticker text,
              start_date timestamp with time zone,
              company_id text,
              cusip text,
              sedol text,
              isin text,
              event_type integer, event_type_name text,
              call_desc text,
              event_title text,
              city text
            );

        CREATE INDEX ON streetevents.calls_raw (file_name, last_update);
        CREATE INDEX ON streetevents.calls_raw (file_path, sha1);
        CREATE INDEX ON streetevents.calls_raw (file_path);

        ALTER TABLE streetevents.calls_raw OWNER TO streetevents;

        GRANT SELECT ON TABLE streetevents.calls_raw TO streetevents_access;")
}
rs <- dbDisconnect(pg)
Sys.setenv(TZ='GMT')

pg <- dbConnect(PostgreSQL())

cat("Updating data on", Sys.getenv("PGHOST"), "\n")

call_files <- tbl(pg, sql("SELECT * FROM streetevents.call_files"))

calls_raw <- tbl(pg, sql("SELECT * FROM streetevents.calls_raw"))

get_file_list <- function() {
    df <-
        call_files %>%
        group_by(file_path) %>%
        filter(ctime == max(ctime, na.rm = TRUE)) %>%
        ungroup() %>%
        select(file_path, sha1) %>%
        anti_join(calls_raw, by = c("file_path", "sha1")) %>%
        select(-sha1) %>%
        collect(n=5000)

    if(nrow(df)>0) pull(df)
}

while (length(file_list <- get_file_list()) > 0) {

    calls_new <- bind_rows(mclapply(file_list,
                                    extract_call_data, mc.cores = 24))

    rs <- dbGetQuery(pg, "SET TIME ZONE 'GMT'")
    if (nrow(calls_new) > 0) {
        rs <- dbWriteTable(pg, c("streetevents", "calls_raw"), calls_new,
                       append = TRUE, row.names = FALSE)
    }
}
print(calls_raw %>% count() %>% pull())

db_comment <- paste0("UPDATED USING import_calls_raw.R from ",
                     "GitHub iangow/se_core ON ", Sys.time())
rs <- dbExecute(pg, paste0("COMMENT ON TABLE streetevents.calls_raw IS '",
                      db_comment, "';"))

rs <- dbDisconnect(pg)
