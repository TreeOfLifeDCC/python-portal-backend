DATA_PORTAL_AGGREGATIONS = [
    "biosamples", "raw_data", "mapped_reads", "assemblies_status",
    "annotation_status", "annotation_complete", "project_name",
    "symbionts_assemblies_status", "symbionts_biosamples_status", "symbionts_raw_data_status",
    "metagenomes_assemblies_status","metagenomes_biosamples_status", "metagenomes_raw_data_status",
    "images_available"
]

ARTICLES_AGGREGATIONS = ["pubYear", "journalTitle", "articleType"]

# Default allowlist of index name prefixes the API is permitted to query.
# Overridable at runtime via the ALLOWED_INDEX_PREFIXES env var
# (comma-separated). Used to stop attacker-controlled path segments from
# targeting arbitrary or system Elasticsearch indices.
ALLOWED_INDEX_PREFIXES = (
    "data_portal",
    "tracking_status_index",
    "articles",
    "summary",
    "organisms_test",
    "specimens_test",
)