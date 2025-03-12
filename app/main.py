import csv
import os
import re
from elasticsearch import AsyncElasticsearch, AIOHttpConnection
from fastapi import FastAPI, Response
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import io


from .constants import DATA_PORTAL_AGGREGATIONS, ARTICLES_AGGREGATIONS

app = FastAPI()

origins = [
    "*"
]

ES_HOST = os.getenv('ES_CONNECTION_URL')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

es = AsyncElasticsearch(
    [ES_HOST],
    timeout=60,
    connection_class=AIOHttpConnection,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    use_ssl=True, verify_certs=False)


@app.get("/{index}")
async def root(index: str, offset: int = 0, limit: int = 15,
               sort: str | None = None, filter: str = None,
               search: str = None, current_class: str = 'kingdom',
               phylogeny_filters: str = None, action: str = None):
    if index == 'favicon.ico':
        return None
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
            name, value = phylogeny_filter.split(":")
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
                _, value = filter_item.split(":")

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
                filter_name, filter_value = filter_item.split(":")

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
                                       body=body, size=50000)
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
        response = await es.search(index=index, q=f"_id:{record_id}")
    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    if 'data_portal' in index:
        data['aggregations'] = aggregations
    return data


class QueryParam(BaseModel):
    pageIndex: int
    pageSize: int
    searchValue: str = ''
    sortValue: str
    filterValue: str = ''
    currentClass: str
    phylogenyFilters: str
    indexName: str
    downloadOption: str


@app.post("/data-download")
async def get_data_files(item: QueryParam):
    data = await root(item.indexName, 0, item.pageSize,
                      item.sortValue, item.filterValue,
                      item.searchValue, item.currentClass,
                      item.phylogenyFilters, 'download')

    csv_data = create_data_files_csv(data['results'], item.downloadOption, item.indexName)

    # Return the byte stream as a downloadable CSV file
    return StreamingResponse(
        csv_data,
        media_type='text/csv',
        headers={"Content-Disposition": "attachment; filename=download.csv"}
    )


def create_data_files_csv(results, download_option, index_name):
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
        header = ['Organism', 'Common Name', 'Common Name Source', 'Current Status']
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
            entry = [organism, common_name, common_name_source, current_status]
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
