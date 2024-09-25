import csv
import os
import re
from io import StringIO
from typing import Optional, List
from elasticsearch import AsyncElasticsearch, AIOHttpConnection
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

import json

from .constants import DATA_PORTAL_AGGREGATIONS

app = FastAPI()

origins = [
    "*"
]

ES_HOST = 'https://prj-ext-dev-dtol-gcp-dr-349815.es.europe-west2.gcp.elastic-cloud.com'
ES_USERNAME = 'elastic'
ES_PASSWORD = 'ZG71gVFpLPB27hTF5xS8P3Vi'

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

es = AsyncElasticsearch(
    [ES_HOST], connection_class=AIOHttpConnection,
    http_auth=(ES_USERNAME, ES_PASSWORD),
    use_ssl=True, verify_certs=False)


@app.get("/downloader_utility_data/")
async def downloader_utility_data(taxonomy_filter: str, data_status: str,
                                  experiment_type: str, project_name: str):
    body = dict()
    if taxonomy_filter != '':

        if taxonomy_filter:
            body["query"] = {
                "bool": {
                    "filter": list()
                }
            }
            nested_dict = {
                "nested": {
                    "path": "taxonomies.class",
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
                        "taxonomies.class.scientificName": taxonomy_filter
                    }
                }
            )
            body["query"]["bool"]["filter"].append(nested_dict)

    if data_status is not None and data_status != '':
        split_array = data_status.split("-")
        if split_array and split_array[0].strip() == 'Biosamples':
            body["query"]["bool"]["filter"].append(
                {"term": {'biosamples': split_array[1].strip()}}
            )
        elif split_array and split_array[0].strip() == 'Raw Data':
            body["query"]["bool"]["filter"].append(
                {"term": {'raw_data': split_array[1].strip()}}
            )
        elif split_array and split_array[0].strip() == 'Mapped Reads':
            body["query"]["bool"]["filter"].append(
                {"term": {'mapped_reads': split_array[1].strip()}})

        elif split_array and split_array[0].strip() == 'Assemblies':
            body["query"]["bool"]["filter"].append(
                {"term": {'assemblies_status': split_array[1].strip()}})
        elif split_array and split_array[0].strip() == 'Annotation Complete':
            body["query"]["bool"]["filter"].append(
                {"term": {'annotation_complete': split_array[1].strip()}})
        elif split_array and split_array[0].strip() == 'Annotation':
            body["query"]["bool"]["filter"].append(
                {"term": {'annotation_status': split_array[1].strip()}})
        elif split_array and split_array[0].strip() == 'Genome Notes':
            nested_dict = {
                "nested": {
                    "path": "genome_notes",
                    "query": {
                        "bool": {
                            "must": [{
                                "exists": {
                                    "field": "genome_notes.url"
                                }
                            }]
                        }
                    }
                }
            }
            body["query"]["bool"]["filter"].append(nested_dict)
    if experiment_type != '' and experiment_type is not None:
        nested_dict = {
            "nested": {
                "path": "experiment",
                "query": {
                    "bool": {
                        "must": [{
                            "term": {
                                "experiment.library_construction_protocol."
                                "keyword": experiment_type
                            }
                        }]
                    }
                }
            }
        }
        body["query"]["bool"]["filter"].append(nested_dict)
    if project_name is not None and project_name != '':
        body["query"]["bool"]["filter"].append(
            {"term": {'project_name': project_name}})
    print(body)
    response = await es.search(index="data_portal", from_=0, size=10000,
                               body=body)
    total_count = response['hits']['total']['value']
    result = response['hits']['hits']
    results_count = len(response['hits']['hits'])
    while total_count > results_count:
        response1 = await es.search(index="data_portal", from_=results_count,
                                    size=10000, body=body)
        result.extend(response1['hits']['hits'])
        results_count += len(response1['hits']['hits'])

    return result


@app.get("/downloader_utility_data_with_species/")
async def downloader_utility_data_with_species(species_list: str,
                                               project_name: str):
    body = dict()
    result = []
    if species_list != '' and species_list is not None:
        species_list_array = species_list.split(",")
        for organism in species_list_array:
            body["query"] = {
                "bool": {"filter": [{'term': {'_id': organism}},
                                    {'term': {'project_name': project_name}}]}}
            response = await es.search(index='data_portal',
                                       body=body)
            result.extend(response['hits']['hits'])

    return result


@app.get("/summary")
async def summary():
    response = await es.search(index="summary")
    data = dict()
    data['results'] = response['hits']['hits']
    return data


def convert_to_title_case(input_string):
    # Add a space before each capital letter
    spaced_string = re.sub(r'([A-Z])', r' \1', input_string)

    # Split the string into words, capitalize each word, and join
    # them with a space
    title_cased_string = ' '.join(
        word.capitalize() for word in spaced_string.split())

    return title_cased_string


@app.get("/detail_table_organism_filters")
async def get_detail_table_organism_filters():
    result = []
    body = {
        "size": 0, "aggregations": {
            "trackingSystem": {"nested": {"path": "trackingSystem"}, "aggs": {
                "rank": {"terms": {"field": "trackingSystem.rank",
                                   "order": {"_key": "desc"}},
                         "aggregations": {
                             "name": {
                                 "terms": {"field": "trackingSystem.name"},
                                 "aggregations": {"status": {"terms": {
                                     "field": "trackingSystem.status"}}}}}}}},
            "symbionts_biosamples_status": {
                "terms": {"field": "symbionts_biosamples_status"}},
            "symbionts_raw_data_status": {
                "terms": {"field": "symbionts_raw_data_status"}},
            "symbionts_assemblies_status": {
                "terms": {"field": "symbionts_assemblies_status"}}}
    }

    data = dict()
    response = await es.search(index='tracking_status_index',
                               size=0, body=body)
    data['buckets'] = response['aggregations']['trackingSystem']['rank'][
        'buckets']


@app.get("/tracking_system_filters")
async def get_filters():
    """

    :return:
    """
    result = []
    body = {
        "size": 0, "aggregations": {
            "trackingSystem": {"nested": {"path": "trackingSystem"}, "aggs": {
                "rank": {"terms": {"field": "trackingSystem.rank",
                                   "order": {"_key": "desc"}},
                         "aggregations": {
                             "name": {
                                 "terms": {"field": "trackingSystem.name"},
                                 "aggregations": {"status": {"terms": {
                                     "field": "trackingSystem.status"}}}}}}}},
            "symbionts_biosamples_status": {
                "terms": {"field": "symbionts_biosamples_status"}},
            "symbionts_raw_data_status": {
                "terms": {"field": "symbionts_raw_data_status"}},
            "symbionts_assemblies_status": {
                "terms": {"field": "symbionts_assemblies_status"}}}
    }

    data = dict()
    response = await es.search(index='tracking_status_index',
                               size=0, body=body)
    data['buckets'] = response['aggregations']['trackingSystem']['rank'][
        'buckets']
    for entry in data['buckets']:
        for name_bucket in entry['name']['buckets']:
            name = name_bucket['key']
            if name == 'annotation_complete' or name == 'biosamples' or name \
                    == 'assemblies' or name == 'raw_data':
                for status_bucket in name_bucket['status']['buckets']:
                    status = status_bucket['key']
                    status_doc_count = status_bucket['doc_count']
                    if status == 'Done':
                        # Append the results
                        result.append({
                            'name': name,
                            'doc_count': status_doc_count
                        })
    result.append({
        'name': 'symbionts_biosamples_status',
        'doc_count': response['aggregations']['symbionts_biosamples_status'][
            'buckets']
    })
    result.append({
        'name': 'symbionts_raw_data_status',
        'doc_count': response['aggregations']['symbionts_raw_data_status'][
            'buckets']
    })
    result.append({
        'name': 'symbionts_assemblies_status',
        'doc_count': response['aggregations']['symbionts_assemblies_status'][
            'buckets']
    })
    return result


@app.get("/export-csv/")
async def export_csv(
        offset: int = 0,
        limit: int = 15,
        sort: str = "rank:desc",
        filter: Optional[str] = None,
        search: Optional[str] = None,
        current_class: str = 'kingdom',
        phylogeny_filters: Optional[str] = None
):
    """
    Exports the current page of records from the specified Elasticsearch index
    as a CSV file.


    :param offset: Starting index for pagination.
    :param limit: Number of records to return.
    :param sort: Sorting criteria (field and order).
    :param filter: Comma-separated filters in the format 'field:value'.
    :param search: Search query string.
    :param current_class: Taxonomy class for filtering.
    :param phylogeny_filters: Filters for phylogeny in the format 'name:value'.
    :return: CSV file as a downloadable response.
    """

    # Build the Elasticsearch query body
    body = {
        "aggs": {
            aggregation_field: {"terms": {"field": aggregation_field}}
            for aggregation_field in DATA_PORTAL_AGGREGATIONS
        },
        "aggs": {
            "taxonomies": {
                "nested": {"path": f"taxonomies.{current_class}"},
                "aggs": {
                    current_class: {
                        "terms": {
                            "field": f"taxonomies."
                                     f"{current_class}.scientificName"}
                    }
                }
            }
        }
    }

    # Apply phylogeny filters
    if phylogeny_filters:
        body.setdefault("query", {"bool": {"filter": []}})
        for phylogeny_filter in phylogeny_filters.split("-"):
            name, value = phylogeny_filter.split(":")
            body["query"]["bool"]["filter"].append({
                "nested": {
                    "path": f"taxonomies.{name}",
                    "query": {"bool": {"filter": [{"term": {
                        f"taxonomies.{name}.scientificName": value}}]}}
                }
            })

    # Apply general filters
    if filter:
        body.setdefault("query", {"bool": {"filter": []}})
        for filter_item in filter.split(","):
            if current_class in filter_item:
                _, value = filter_item.split(":")
                body["query"]["bool"]["filter"].append({
                    "nested": {
                        "path": f"taxonomies.{current_class}",
                        "query": {"bool": {"filter": [{"term": {
                            f"taxonomies.{current_class}."
                            f"scientificName": value}}]}}
                    }
                })
            else:
                filter_name, filter_value = filter_item.split(":")
                body["query"]["bool"]["filter"].append(
                    {"term": {filter_name: filter_value}})

    # Apply search string
    if search:
        body.setdefault("query", {"bool": {"must": {"bool": {"should": []}}}})
        body["query"]["bool"]["must"]["bool"]["should"].extend([
            {"wildcard": {"organism": {"value": f"*{search}*",
                                       "case_insensitive": True}}},
            {"wildcard": {"commonName": {"value": f"*{search}*",
                                         "case_insensitive": True}}},
            {"wildcard": {
                "symbionts_records.organism.text": {"value": f"*{search}*",
                                                    "case_insensitive": True}}}
        ])

    # Perform the Elasticsearch search query
    response = await es.search(index='data_portal', sort=sort, from_=offset,
                               size=limit, body=body)
    hits = response['hits']['hits']

    if not hits:
        return {"message": "No data found."}

    # Define the columns and their title case versions
    columns = {'organism', 'commonName', 'commonNameSource', 'currentStatus'}
    title_case_columns = [convert_to_title_case(col) for col in columns]

    # Prepare CSV output
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=title_case_columns)
    writer.writeheader()

    for hit in hits:
        row = {convert_to_title_case(col): hit['_source'].get(col, '') for col
               in columns}
        writer.writerow(row)

    output.seek(0)

    headers = {
        'Content-Disposition': 'attachment; filename="data_portal.csv"',
        'Content-Type': 'text/csv'
    }

    return Response(content=output.getvalue(), media_type="text/csv",
                    headers=headers)


@app.get("/{index}")
async def root(index: str, offset: int = 0, limit: int = 15,
               sort: str = "currentSt1ssaxn1atus:asc", filter: str = None,
               search: str = None, current_class: str = 'kingdom',
               phylogeny_filters: str = None):
    global value
    print(phylogeny_filters)
    # data structure for ES query
    body = dict()
    # building aggregations for every request
    body["aggs"] = dict()

    for aggregation_field in DATA_PORTAL_AGGREGATIONS:
        body["aggs"][aggregation_field] = {
            "terms": {"field": aggregation_field + '.keyword'}
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
    body["aggs"]["experiment"] = {
        "nested": {"path": "experiment"},
        "aggs": {"library_construction_protocol": {"terms": {
            "field": "experiment.library_construction_protocol.keyword"}
        }
        }
    }
    body["aggs"]["genome"] = {
        "nested": {"path": "genome_notes"},
        "aggs": {"genome_count": {"cardinality": {"field": "genome_notes.id"}
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

    # adding search string
    if search:
        # body already has filter parameters
        if "query" in body:
            # body["query"]["bool"].update({"should": []})
            body["query"]["bool"].update({"must": {}})
        else:
            # body["query"] = {"bool": {"should": []}}
            body["query"] = {"bool": {"must": {}}}
        body["query"]["bool"]["must"] = {"bool": {"should": []}}
        body["query"]["bool"]["must"]["bool"]["should"].append(
            {"wildcard": {"organism": {"value": f"*{search}*",
                                       "case_insensitive": True}}}
        )
        body["query"]["bool"]["must"]["bool"]["should"].append(
            {"wildcard": {"commonName": {"value": f"*{search}*",
                                         "case_insensitive": True}}}
        )
        body["query"]["bool"]["must"]["bool"]["should"].append(
            {"wildcard": {"symbionts_records.organism.text":
                              {"value": f"*{search}*",
                               "case_insensitive": True}}}
        )
    print(json.dumps(body))
    response = await es.search(
        index=index, sort=sort, from_=offset, size=limit, body=body
    )
    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    data['aggregations'] = response['aggregations']
    return data


@app.get("/{index}/{record_id}")
async def details(index: str, record_id: str):
    body = dict()
    if index == 'data_portal':
        body["query"] = {
            "bool": {"filter": [{'term': {'organism': record_id}}]}}
        body["aggregations"] = {'filters': {'nested': {'path': 'records'},
                                            "aggs": {
                                                'sex_filter': {'terms': {
                                                    'field': 'records.sex',
                                                    'size': 2000}},
                                                'tracking_status_filter': {
                                                    'terms': {
                                                        'field': 'records'
                                                                 '.trackingSystem',
                                                        'size': 2000}},
                                                'organism_part_filter': {
                                                    'terms': {
                                                        'field': 'records'
                                                                 '.organismPart',
                                                        'size': 2000}}
                                            }}}
        response = await es.search(index=index, body=body)
        aggregations = response['aggregations']
    else:
        response = await es.search(index=index, q=f"_id:{record_id}")
    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    if index == 'data_portal':
        data['aggregations'] = aggregations
    return data
