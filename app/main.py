import csv
import io
import os
import re

from elasticsearch import AsyncElasticsearch, AIOHttpConnection, ConnectionTimeout
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from starlette.responses import JSONResponse

from .constants import (
    DATA_PORTAL_AGGREGATIONS,
    ARTICLES_AGGREGATIONS,
    ALLOWED_INDEX_PREFIXES,
)

app = FastAPI()

# Public, unauthenticated API: any origin may read it. A wildcard origin is
# safe here ONLY because credentials are disabled below -- "*" must never be
# combined with allow_credentials=True.
origins = ["*"]

ES_HOST = os.getenv('ES_CONNECTION_URL')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')
# TLS certificate verification is ON by default to protect the ES credentials
# in transit. For a cluster using a self-signed cert, set ES_VERIFY_CERTS=false.
ES_VERIFY_CERTS = os.getenv('ES_VERIFY_CERTS', 'true').lower() != 'false'

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Allowlists constraining attacker-controlled input that would otherwise reach
# Elasticsearch as a target index or as a query field name.
_raw_index_prefixes = os.getenv('ALLOWED_INDEX_PREFIXES', '')
ALLOWED_PREFIXES = tuple(
    p.strip() for p in _raw_index_prefixes.split(',') if p.strip()
) or ALLOWED_INDEX_PREFIXES
INDEX_NAME_RE = re.compile(r'^[a-z0-9][a-z0-9_-]*$')
TAXONOMY_RANK_RE = re.compile(r'^[a-zA-Z_]+$')
FIELD_NAME_RE = re.compile(r'^[a-zA-Z0-9_.]+$')


def validate_index(index: str) -> str:
    """Reject any index that is not an allowlisted portal index.

    Blocks reads against arbitrary or system indices and rejects ES
    multi-index / wildcard expressions (``*``, ``_all``, comma lists and
    leading-dot indices are all excluded by the character class).
    """
    if (
        not index
        or not INDEX_NAME_RE.match(index)
        or not index.startswith(ALLOWED_PREFIXES)
    ):
        raise HTTPException(status_code=404, detail="Unknown index")
    return index


def validate_field_name(value: str, pattern: "re.Pattern") -> str:
    """Reject field/path names that contain anything outside the allowlist."""
    if not value or not pattern.match(value):
        raise HTTPException(status_code=400, detail="Invalid query parameter")
    return value


es = AsyncElasticsearch(
    [ES_HOST],
    timeout=60,
    connection_class=AIOHttpConnection,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    use_ssl=True,
    verify_certs=ES_VERIFY_CERTS,
    ssl_show_warn=ES_VERIFY_CERTS,
)


@app.get("/{index}")
async def root(index: str, offset: int = 0, limit: int = 15,
               sort: str | None = None, filter: str = None,
               search: str = None, current_class: str = 'kingdom',
               phylogeny_filters: str = None, action: str = None):
    if index == 'favicon.ico':
        return None
    validate_index(index)
    validate_field_name(current_class, TAXONOMY_RANK_RE)
    global value

    # data structure for ES query
    body = dict()
    # building aggregations for every request
    body["aggs"] = dict()

    if 'articles' in index:
        aggregations_list = ARTICLES_AGGREGATIONS
    else:
        aggregations_list = DATA_PORTAL_AGGREGATIONS

    for aggregation_field in aggregations_list:
        if aggregation_field == 'images_available':
            body["aggs"][aggregation_field] = {
                "terms": {"field": aggregation_field}
            }
        else:
            body["aggs"][aggregation_field] = {
                "terms": {"field": aggregation_field + '.keyword'}
            }

    if 'data_portal' in index:
        body["aggs"]["experiment"] = {
            "nested": {"path": "experiment"},
            "aggs": {
                "library_construction_protocol": {
                    "terms": {
                        "field": "experiment.library_construction_protocol.keyword",
                    },
                    "aggs": {
                        "distinct_docs": {
                            "reverse_nested": {},
                            "aggs": {
                                "parent_doc_count": {
                                    "cardinality": {
                                        "field": "organism.keyword"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if 'data_portal' in index or 'tracking_status' in index:
            body["aggs"]["genome_notes"] = {
                "nested": {"path": "genome_notes"},
                "aggs": {
                    "genome_count": {
                        "reverse_nested": {},  # get to the parent document level
                        "aggs": {
                            "distinct_docs": {
                                "cardinality": {
                                    "field": "organism.keyword"
                                }
                            }
                        }
                    }
                }
            }

    body["aggs"]["taxonomies"] = {
        "nested": {"path": f"taxonomies.{current_class}"},
        "aggs": {current_class: {
            "terms": {
                "field": f"taxonomies.{current_class}.scientificName"
            }
        }
        }
    }


    if phylogeny_filters:
        body["query"] = {
            "bool": {
                "filter": list()
            }
        }
        phylogeny_filters = phylogeny_filters.split("-")
        print(phylogeny_filters)
        for phylogeny_filter in phylogeny_filters:
            name, value = phylogeny_filter.split(":", 1)
            validate_field_name(name, TAXONOMY_RANK_RE)
            nested_dict = {
                "nested": {
                    "path": f"taxonomies.{name}",
                    "query": {
                        "bool": {
                            "filter": list()
                        }
                    }
                }
            }
            nested_dict["nested"]["query"]["bool"]["filter"].append(
                {
                    "term": {
                        f"taxonomies.{name}.scientificName": value
                    }
                }
            )
            body["query"]["bool"]["filter"].append(nested_dict)
    # adding filters, format: filter_name1:filter_value1, etc...
    if filter:
        filters = filter.split(",")
        if 'query' not in body:
            body["query"] = {
                "bool": {
                    "filter": list()
                }
            }
        for filter_item in filters:
            if current_class in filter_item:
                _, value = filter_item.split(":", 1)

                nested_dict = {
                    "nested": {
                        "path": f"taxonomies.{current_class}",
                        "query": {
                            "bool": {
                                "filter": list()
                            }
                        }
                    }
                }
                nested_dict["nested"]["query"]["bool"]["filter"].append(
                    {
                        "term": {
                            f"taxonomies.{current_class}.scientificName": value
                        }
                    }
                )
                body["query"]["bool"]["filter"].append(nested_dict)

            else:
                filter_name, filter_value = filter_item.split(":", 1)
                validate_field_name(filter_name, FIELD_NAME_RE)

                if filter_name == 'experimentType':

                    nested_dict = {
                        "nested": {
                            "path": "experiment",
                            "query": {
                                "bool": {
                                    "filter": {
                                        "term": {
                                            "experiment"
                                            ".library_construction_protocol"
                                            ".keyword": filter_value
                                        }
                                    }
                                }
                            }
                        }
                    }
                    body["query"]["bool"]["filter"].append(nested_dict)
                elif filter_name == 'genome_notes':
                    nested_dict = {
                        'nested': {'path': 'genome_notes', 'query': {
                            'bool': {
                                'must': [
                                    {'exists': {
                                        'field': 'genome_notes.url'}}]}}}}
                    body["query"]["bool"]["filter"].append(nested_dict)
                else:
                    print(filter_name)
                    body["query"]["bool"]["filter"].append(
                        {"term": {filter_name: filter_value}})

    # Adding search string
    if search:
        if "query" not in body:
            body["query"] = {"bool": {"must": {"bool": {"should": []}}}}
        else:
            body["query"]["bool"].setdefault("must", {"bool": {"should": []}})

        search_fields = (
            ["title", "journal_name", "study_id", "organism_name"]
            if 'articles' in index
            else ["organism", "commonName", "symbionts_records.organism.text", "metagenomes_records.organism.text"]
        )

        for field in search_fields:
            body["query"]["bool"]["must"]["bool"]["should"].append({
                "wildcard": {
                    field: {
                        "value": f"*{search}*",
                        "case_insensitive": True
                    }
                }
            })

    if action == 'download':
        try:
            response = await es.search(index=index, sort=sort, from_=offset,
                                       body=body, size=limit)
        except ConnectionTimeout:
            return {"error": "Request to Elasticsearch timed out."}
    else:
        response = await es.search(index=index, sort=sort, from_=offset, size=limit, body=body)

    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    data['aggregations'] = response['aggregations']
    return data


@app.get("/{index}/{record_id}")
async def details(index: str, record_id: str):
    validate_index(index)
    body = dict()
    if 'data_portal' in index:
        body["query"] = {
            "bool": {
                "filter": [
                    {
                        'term': {
                            'organism': record_id
                        }
                    }
                ]
            }
        }
        body["aggs"] = dict()
        body["aggs"]["metadata_filters"] = {
            'nested': {'path': 'records'},
            "aggs": {
                'sex_filter': {
                    'terms': {
                        'field':
                            'records.sex.keyword',
                        'size': 2000}},
                'tracking_status_filter': {
                    'terms': {
                        'field':
                            'records.'
                            'trackingSystem.keyword',
                        'size': 2000}},
                'organism_part_filter': {
                    'terms': {
                        'field': 'records'
                                 '.organismPart.keyword',
                        'size': 2000}}
            }}
        body["aggs"]["symbionts_filters"] = {
            'nested': {'path': 'symbionts_records'},
            "aggs": {
                'sex_filter': {
                    'terms': {
                        'field':
                            'symbionts_records.sex.keyword',
                        'size': 2000}},
                'tracking_status_filter': {
                    'terms': {
                        'field':
                            'symbionts_records.'
                            'trackingSystem.keyword',
                        'size': 2000}},
                'organism_part_filter': {
                    'terms': {
                        'field': 'symbionts_records'
                                 '.organismPart.keyword',
                        'size': 2000}}
            }}
        body['aggs']['metagenomes_filters'] = {
            'nested': {'path': 'metagenomes_records'},
            "aggs": {
                'sex_filter': {
                    'terms': {
                        'field':
                            'metagenomes_records.sex.keyword',
                        'size': 2000}},
                'tracking_status_filter': {
                    'terms': {
                        'field':
                            'metagenomes_records.'
                            'trackingSystem.keyword',
                        'size': 2000}},
                'organism_part_filter': {
                    'terms': {
                        'field': 'metagenomes_records'
                                 '.organismPart.keyword',
                        'size': 2000}}
            }}

        response = await es.search(index=index, body=body)
        aggregations = response['aggregations']
    else:
        response = await es.search(
            index=index, body={"query": {"ids": {"values": [record_id]}}})
    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    if 'data_portal' in index:
        data['aggregations'] = aggregations
    return data


@app.get("/summary")
async def summary():
    response = await es.search(index="summary")
    data = dict()
    data['results'] = response['hits']['hits']
    return data


class QueryParam(BaseModel):
    pageIndex: int
    pageSize: int
    searchValue: str = ''
    sortValue: str
    filterValue: str = ''
    currentClass: str
    phylogeny_filters: str
    index_name: str
    downloadOption: str


@app.post("/data-download")
async def get_data_files(item: QueryParam):
    data = await fetch_data_in_batches(item)
    if len(data) > 0:
        csv_data = create_data_files_csv(data, item.downloadOption,
                                         item.index_name)

        return StreamingResponse(
            csv_data,
            media_type='text/csv',
            headers={"Content-Disposition": "attachment; filename=download.csv"}
        )
    else:
        return JSONResponse(
            status_code=500,
            content={"error": "There was an issue downloading the file"}
        )


async def fetch_data_in_batches(item: QueryParam):
    offset = 0
    batch_size = 1000
    all_data = []

    while True:

        data = await root(
            item.index_name, offset, batch_size,
            item.sortValue, item.filterValue,
            item.searchValue, item.currentClass,
            item.phylogeny_filters, 'download'
        )


        results = data.get('results', [])
        if not results:
            break

        all_data.extend(results)
        offset += batch_size
        print(f"Fetched {len(results)} results, total: {len(all_data)}")

    return all_data


def create_data_files_csv(results, download_option, index_name):
    INSDC_ID = ''
    tolid_str = ''
    header = []
    if download_option.lower() == "assemblies":
        header = ["Scientific Name", "Accession", "Version", "Assembly Name", "Assembly Description",
                  "Link to chromosomes, contigs and scaffolds all in one"]
    elif download_option.lower() == "annotation":
        header = ["Annotation GTF", "Annotation GFF3", "Proteins Fasta", "Transcripts Fasta",
                  "Softmasked genomes Fasta"]
    elif download_option.lower() == "raw_files":
        header = ["Study Accession", "Sample Accession", "Experiment Accession", "Run Accession", "Tax Id",
                  "Scientific Name", "FASTQ FTP", "Submitted FTP", "SRA FTP", "Library Construction Protocol"]
    elif download_option.lower() == "metadata" and 'data_portal' in index_name:
        header = ['Organism', 'Common Name', 'Common Name Source', 'Current Status','INSDC ID', 'ToL ID']
    elif download_option.lower() == "metadata" and 'tracking_status' in index_name:
        header = ['Organism', 'Common Name', 'Metadata submitted to BioSamples', 'Raw data submitted to ENA',
                  'Mapped reads submitted to ENA', 'Assemblies submitted to ENA',
                  'Annotation complete', 'Annotation submitted to ENA']

    output = io.StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(header)

    for entry in results:
        record = entry["_source"]
        if download_option.lower() == "assemblies":
            assemblies = record.get("assemblies", [])
            scientific_name = record.get("organism", "")
            for assembly in assemblies:
                accession = assembly.get("accession", "-")
                version = assembly.get("version", "-")
                assembly_name = assembly.get("assembly_name", "")
                assembly_description = assembly.get("description", "")
                link = f"https://www.ebi.ac.uk/ena/browser/api/fasta/{accession}?download=true&gzip=true" if accession else ""
                entry = [scientific_name, accession, version, assembly_name, assembly_description, link]
                csv_writer.writerow(entry)

        elif download_option.lower() == "annotation":
            annotations = record.get("annotation", [])
            print("annotations: ", annotations)
            for annotation in annotations:
                gtf = annotation.get("annotation", {}).get("GTF", "-")
                gff3 = annotation.get("annotation", {}).get("GFF3", "-")
                proteins_fasta = annotation.get("proteins", {}).get("FASTA", "")
                transcripts_fasta = annotation.get("transcripts", {}).get("FASTA", "")
                softmasked_genomes_fasta = annotation.get("softmasked_genome", {}).get("FASTA", "")
                entry = [gtf, gff3, proteins_fasta, transcripts_fasta, softmasked_genomes_fasta]
                csv_writer.writerow(entry)

        elif download_option.lower() == "raw_files":
            experiments = record.get("experiment", [])
            for experiment in experiments:
                study_accession = experiment.get("study_accession", "")
                sample_accession = experiment.get("sample_accession", "")
                experiment_accession = experiment.get("experiment_accession", "")
                run_accession = experiment.get("run_accession", "")
                tax_id = experiment.get("tax_id", "")
                scientific_name = experiment.get("scientific_name", "")
                submitted_ftp = experiment.get("submitted_ftp", "")
                sra_ftp = experiment.get("sra-ftp", "")
                library_construction_protocol = experiment.get("library_construction_protocol", "")
                fastq_ftp = experiment.get("fastq_ftp", "")

                if fastq_ftp:
                    fastq_list = fastq_ftp.split(";")
                    for fastq in fastq_list:
                        entry = [study_accession, sample_accession, experiment_accession, run_accession, tax_id,
                                 scientific_name, fastq, submitted_ftp, sra_ftp, library_construction_protocol]
                        csv_writer.writerow(entry)
                else:
                    entry = [study_accession, sample_accession, experiment_accession, run_accession, tax_id,
                             scientific_name, fastq_ftp, submitted_ftp, sra_ftp, library_construction_protocol]
                    csv_writer.writerow(entry)

        elif download_option.lower() == "metadata" and 'data_portal' in index_name:
            organism = record.get('organism', '')
            common_name = record.get('commonName', '')
            common_name_source = record.get('commonNameSource', '')
            current_status = record.get('currentStatus', '')
            tolids = record.get('tolid', [])
            print(tolids)
            if tolids:
                tolid_str = ", ".join(map(str, tolids))
            experiments = record.get("experiment", [])
            if experiments:
                INSDC_ID = experiments[0].get("study_accession", "")
            entry = [organism, common_name, common_name_source, current_status, INSDC_ID, tolid_str]
            csv_writer.writerow(entry)

        elif download_option.lower() == "metadata" and 'tracking_status' in index_name:
            organism = record.get('organism', '')
            common_name = record.get('commonName', '')
            metadata_biosamples = record.get('biosamples', '')
            raw_data_ena = record.get('raw_data', '')
            mapped_reads_ena = record.get('mapped_reads', '')
            assemblies_ena = record.get('assemblies_status', '')
            annotation_complete = record.get('annotation_complete', '')
            annotation_submitted_ena = record.get('annotation_status', '')
            entry = [organism, common_name, metadata_biosamples, raw_data_ena, mapped_reads_ena, assemblies_ena,
                     annotation_complete, annotation_submitted_ena]
            csv_writer.writerow(entry)

    output.seek(0)
    return io.BytesIO(output.getvalue().encode('utf-8'))
