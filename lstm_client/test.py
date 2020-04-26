from elasticsearch import Elasticsearch


def a():
    b_uid="f626f9e3-c2e7-4c01-70f0-39f780dcc022"
    es = Elasticsearch([{
                    "host": "192.168.6.33",
                    "port": 9200
                }])
    body = {
        # "sort":[{
        #     "occ_time":{"order" : "asc"}
        # }],
        "query": {
            "bool": {
                "must": [
                    {"match": {"status": "2"}}
                ]
            }
        }
    }
    result = es.search(index="lstm", body=body)
    limit = result["hits"]["total"]
    body = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"status": "2"}}
                ]
            }
        },
        "size": limit
    }
    result = es.search(index="lstm", body=body)
    for list in result["hits"]["hits"]:
        key = list["_id"]
        es.update(index="lstm", doc_type="_doc", id=key, body={"doc": {"status": "0"}})
    return
print(a())

