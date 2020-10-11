from elasticsearch import Elasticsearch
es = Elasticsearch()


def bucketAggBody(agg_name, agg_type, agg_parameters):

    query_body={}
    query_body["aggs"]={
        agg_name: {
        agg_type: agg_parameters
        }
    }
    return query_body

class Agg:

    def terms(parameters):

        agg_parameters={"field": parameters["field"]}
        return bucketAggBody("term_agg", "terms", agg_parameters)

    def geoDistance(parameters):

        agg_parameters={
            "field": parameters["field"],
            "origin": parameters["origin"],
            "ranges": parameters["ranges"],
        }
        return bucketAggBody("geo_distance_agg", "geo_distance", agg_parameters)

    def dateRange(parameters):

        agg_parameters={
            "field": parameters["field"],
            "ranges": parameters["ranges"],
        }
        return bucketAggBody("date_range_agg", "date_range", agg_parameters)