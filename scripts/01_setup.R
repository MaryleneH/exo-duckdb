
# scripts/01_setup.R
# Connexion DuckDB, extensions, variables S3 & Trino (via .env)

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(glue)
  library(readr)
})

# Charger .env si présent
env_file <- file.path("config", ".env")
if (file.exists(env_file)) {
  lines <- readLines(env_file, warn = FALSE)
  for (ln in lines) {
    if (nchar(ln) == 0 || startsWith(ln, "#")) next
    kv <- strsplit(ln, "=", fixed = TRUE)[[1]]
    if (length(kv) == 2) Sys.setenv(kv[1] = kv[2])
  }
}

con <- dbConnect(duckdb::duckdb())

# Appliquer PRAGMAs
pragmas_path <- file.path("config", "duckdb_pragmas.sql")
if (file.exists(pragmas_path)) {
  sql_txt <- paste(readLines(pragmas_path, warn = FALSE), collapse = "\n")
  dbExecute(con, sql_txt)
}

# Installer/Charger httpfs & parquet
dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
dbExecute(con, "INSTALL parquet; LOAD parquet;")

# Config S3/MinIO si variables définies
cfg <- Sys.getenv(c("S3_REGION","S3_ENDPOINT","S3_ACCESS_KEY_ID","S3_SECRET_ACCESS_KEY"))
if (all(nchar(cfg) > 0)) {
  dbExecute(con, glue("SET s3_region='{Sys.getenv('S3_REGION')}';"))
  dbExecute(con, glue("SET s3_endpoint='{Sys.getenv('S3_ENDPOINT')}';"))
  dbExecute(con, glue("SET s3_access_key_id='{Sys.getenv('S3_ACCESS_KEY_ID')}';"))
  dbExecute(con, glue("SET s3_secret_access_key='{Sys.getenv('S3_SECRET_ACCESS_KEY')}';"))
}

message("DuckDB prêt. Con: 'con'")
assign("con", con, envir = .GlobalEnv)

# Helper pour vérifier une table/chemin rapidement
peek_parquet <- function(path, n = 5) {
  stopifnot(DBI::dbIsValid(con))
  DBI::dbGetQuery(con, glue("SELECT * FROM parquet_scan('{path}') LIMIT {n}"))
}
assign("peek_parquet", peek_parquet, envir = .GlobalEnv)

message("Helpers chargés: peek_parquet(path, n = 5)")
