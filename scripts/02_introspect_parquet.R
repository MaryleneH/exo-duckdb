
# scripts/02_introspect_parquet.R
# Schéma, métadonnées, statistiques (row groups) à l'échelle

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(glue)
  library(dplyr)
})

if (!exists("con") || !DBI::dbIsValid(con)) {
  stop("Veuillez exécuter d'abord scripts/01_setup.R pour créer 'con'.")
}

# Chemins
path_local <- Sys.getenv("PARQUET_PATH_LOCAL", unset = "data/*.parquet")
path_s3    <- Sys.getenv("PARQUET_PATH_S3", unset = "")

target <- if (nchar(path_s3) > 0) path_s3 else path_local
message(glue("Cible: {target}"))

# Aperçu schéma d'un fichier (si local connu) — à adapter si nécessaire
sample_query <- glue("SELECT * FROM parquet_scan('{target}') LIMIT 1;")
preview <- tryCatch(DBI::dbGetQuery(con, sample_query), error = function(e) NULL)
if (!is.null(preview)) print(utils::head(preview, 1))

# Fonction d'introspection (schéma + stats par colonne)
introspect_one <- function(file) {
  schema <- DBI::dbGetQuery(con, glue("SELECT * FROM parquet_schema('{file}')"))
  meta   <- DBI::dbGetQuery(con, glue("SELECT * FROM parquet_metadata('{file}')"))
  list(schema = schema, meta = meta)
}

# Exemple: récupérer de la métadonnée agrégée pour tout un dataset (via wildcard)
meta_all <- DBI::dbGetQuery(con, glue("
  WITH m AS (
    SELECT * FROM parquet_metadata('{target}')
  )
  SELECT
    column,
    COUNT(DISTINCT file)                          AS files,
    COUNT(DISTINCT row_group)                     AS row_groups,
    SUM(total_uncompressed_size)                  AS uncompressed_bytes,
    SUM(total_compressed_size)                    AS compressed_bytes,
    SUM(num_values)                               AS total_values,
    MIN(statistics_min)                           AS global_min,
    MAX(statistics_max)                           AS global_max
  FROM m
  GROUP BY column
  ORDER BY uncompressed_bytes DESC;
"))

print(meta_all)

# Export CSV des métriques
if (nrow(meta_all) > 0) {
  utils::write.csv(meta_all, file = "out_parquet_profile.csv", row.names = FALSE)
  message("Écrit: out_parquet_profile.csv")
}
