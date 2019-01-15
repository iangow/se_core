#!/usr/bin/env Rscript
cat("Importing speaker data.\n")
library(xml2)
library(stringr)
library(dplyr, warn.conflicts = FALSE)
library(parallel)

se_path <- file.path(Sys.getenv("SE_DIR"))
Sys.setenv(TZ='GMT')

is.error <- function(x) inherits(x, "try-error")

unescape_xml <- function(str) {
    xml_text(read_html(paste0("<x>", str, "</x>")))
}

extract_speaker_data <- function(file_path) {

    full_path <- file.path(se_path, file_path)
    if (!file.exists(full_path)) return(NULL)
    file_name <- str_replace(basename(file_path), "\\.xml$", "")

    read_data <- function(se_path, file_path) {
            file_xml <- read_xml(file.path(se_path, file_path), options = "NOENT")
            last_update <- as.POSIXct(xml_attr(file_xml, "lastUpdate"),
                                      format="%A, %B %d, %Y at %H:%M:%S%p GMT", tz = "GMT")
            lines <- xml_text(xml_child(file_xml, search = "/EventStory/Body"))
            lines <- gsub("\\r\\n", "\n", lines, perl = TRUE)
            lines <- unescape_xml(lines)

            sections <- str_split(lines, "==={3,}\n")[[1]]

            analyze_text <- function(section_text) {
                values <- str_split(section_text, "\n---{3,}")
                text <- str_trim(values[[1]])
                num_speakers <- (length(text) - 1)/2
                speaker_data <- text[seq(from = 2, length.out = num_speakers, by = 2)]
                speaker_text <- text[seq(from = 3, length.out = num_speakers, by = 2)]
                speaker <- clean_speaker(speaker_data)
                bind_cols(extract_speaker(speaker), tibble(speaker_text))
            }

            analyze_text_wrap <- function(sections) {
                bind_rows(lapply(sections, analyze_text), .id="section")
            }

            clean_speaker <- function(speaker) {
                speaker <- gsub("\\n", " ", speaker)
                speaker <- gsub("\\s{2,}", " ", speaker)
                speaker <- str_trim(speaker)
                speaker <- str_replace_all(speaker, "\\t+", "")
                return(speaker)
            }

            extract_speaker <- function(speaker) {
                temp2 <- str_match(speaker, "^(.*)\\s+\\[(\\d+)\\]")
                temp3 <- str_match(speaker, "^.*\\[(\\d+)\\]")
                if (dim(temp2)[2] >= 3) {
                    speaker_number <- temp2[, 3]
                    speaker_number <- if_else(is.na(speaker_number), temp3[, 2], speaker_number)
                    full_name <- temp2[, 2]

                    spaces <- "[\\s\\p{WHITE_SPACE}\u3000\ua0]"
                    regex <- str_c("^([^,]*),", spaces, "*(.*)", spaces, "+-", spaces,
                                   "+(.*)$")
                    temp3 <- str_match(full_name, regex)
                    if (dim(temp3)[2] >= 4) {
                        speaker_name <- if_else(is.na(full_name), full_name,
                                                str_trim(temp3[, 2]))
                        speaker_name <- str_trim(speaker_name)
                        employer <- str_trim(coalesce(temp3[, 3], ""))
                        role <- str_trim(coalesce(temp3[, 4], ""))
                    } else {
                        speaker_name <- NA
                        employer <- NA
                        role <- NA
                    }
                    speaker_name <- if_else(is.na(speaker_name),
                                            str_trim(temp2[, 2]), speaker_name)
                } else {
                    speaker_number <- NA
                    speaker_name <- NA
                    employer <- NA
                    role <- NA
                }

                tibble(file_name, last_update, speaker_name,
                       employer, role, speaker_number)
        }

        pres <- sections[grepl("^(Presentation|Transcript|presentation\\.)\\s*\n", sections)]

        if (length(pres) > 0) {
            pres_df <-
                analyze_text_wrap(pres) %>%
                mutate(context = "pres") %>%
                select(file_name, last_update, speaker_name, employer, role,
                       speaker_number, speaker_text, context, section)
        } else {
            return(NULL)
        }

        qa <- sections[grepl("^(Questions and Answers|q and a)", sections)]

        if (length(qa) > 0) {
            qa_df <-
                analyze_text_wrap(qa) %>%
                mutate(context = "qa") %>%
                select(file_name, last_update, speaker_name, employer, role,
                       speaker_number, speaker_text, context, section)
            return(bind_rows(pres_df, qa_df))
        } else {
            return(pres_df)
        }
    }

    empty_df <- tibble(file_name)

    file_data <- try(read_data(se_path, file_path))
    if (is.error(file_data) | is.null(file_data)) return(empty_df)
    return(file_data)
}

# Get a list of files that need to be processed ----
library(DBI)
pg <- dbConnect(RPostgres::Postgres(), bigint = "integer")

rs <- dbExecute(pg, "SET search_path TO streetevents, public")

if (!dbExistsTable(pg, "speaker_data")) {
    dbExecute(pg, "
        CREATE TABLE streetevents.speaker_data
           (
           file_name text,
           last_update timestamp with time zone,
           speaker_name text,
           employer text,
           role text,
           speaker_number integer,
           speaker_text text,
           context text,
           section integer,
        PRIMARY KEY (file_name, last_update, speaker_number, context, section));

       ALTER TABLE streetevents.speaker_data OWNER TO streetevents;
       GRANT SELECT ON streetevents.speaker_data TO streetevents_access;
       CREATE INDEX ON streetevents.speaker_data (file_name, last_update);")
}

if (!dbExistsTable(pg, "speaker_data_dupes")) {
    dbExecute(pg, "
        CREATE TABLE streetevents.speaker_data_dupes
               (file_name text, last_update timestamp with time zone);
        GRANT SELECT ON streetevents.speaker_data_dupes TO streetevents_access;
        ALTER TABLE streetevents.speaker_data_dupes OWNER TO streetevents;")
}

rs <- dbDisconnect(pg)

process_calls <- function(num_calls = 1000, file_list = NULL) {
    pg <- dbConnect(RPostgres::Postgres(), bigint = "integer")
    
    rs <- dbExecute(pg, "SET work_mem = '3GB'")
    rs <- dbExecute(pg, "SET search_path TO streetevents")

    selected_calls <- tbl(pg, "selected_calls")
    speaker_data <- tbl(pg, "speaker_data")
    speaker_data_dupes <- tbl(pg, "speaker_data_dupes")
    speaker_data_all <-
        speaker_data %>%
        select(file_name, last_update) %>%
        union_all(
            speaker_data_dupes %>%
                      select(file_name, last_update))
    
    if (is.null(file_list)) {

        file_list <-
            selected_calls %>%
            anti_join(speaker_data_all, by = c("file_name", "last_update")) %>%
            distinct() %>%
            collect(n = num_calls) 
    }
    if (nrow(file_list)==0) return(FALSE)
    temp <- mclapply(file_list$file_path, extract_speaker_data, mc.cores=12)

    print(length(temp))

    temp_df <- bind_rows(temp)

    if ("speaker_text" %in% colnames(temp_df)) {

        speaker_data_new <-
            temp_df %>%
            filter(speaker_text != "")

        print(sprintf("Speaker data has %d rows", nrow(speaker_data_new)))

        if (nrow(speaker_data_new) == 0) {
            dupes <- file_list
        } else {
            dupes <-
                speaker_data_new %>%
                group_by(file_name, last_update, speaker_number, context, section) %>%
                filter(n() > 1) %>%
                ungroup() %>%
                union_all(
                    speaker_data_new %>%
                        filter(is.na(speaker_number) | is.na(context) | is.na(section)))

            speaker_data_new <-
                speaker_data_new %>%
                anti_join(dupes, by=c("file_name", "last_update"))
        }

        rs <- dbExecute(pg, "SET TIME ZONE 'GMT'")
        if (nrow(speaker_data_new) > 0) {
            print("Writing data to Postgres")

            dbWriteTable(pg, "speaker_data", speaker_data_new, 
                         row.names=FALSE, append=TRUE)
        }
        print("Writing dupe data to Postgres")

        if (nrow(dupes) > 0) {
            file_path <-
                dupes %>%
                select(file_name, last_update) %>%
                distinct()

            dbWriteTable(pg, "speaker_data_dupes", file_path,
                         row.names=FALSE, append=TRUE)
        }

        rs <- dbDisconnect(pg)
        return(nrow(file_list)>0)
    } else {
        rs <- dbExecute(pg, "SET TIME ZONE 'GMT'")
        dupes <-
            file_list %>%
            select(file_name, last_update) %>%
            distinct()
        dbWriteTable(pg, "speaker_data_dupes", dupes,
                     row.names=FALSE, append=TRUE)
        rs <- dbDisconnect(pg)
        return(FALSE)
    }
}

system.time(while(tm <- process_calls(num_calls = 5000)) {
    cat("New rows: ", tm, "\n")
})
