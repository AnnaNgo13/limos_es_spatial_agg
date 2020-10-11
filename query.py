from bucket_agg import *
from datetime import datetime, timedelta

def initiateQuery(body):

    return body

def addSubAgg(query, body):

    pointer=query
    while "aggs" in pointer.keys():
        agg_name=list(pointer["aggs"].keys())[0]
        pointer=pointer["aggs"][agg_name]
    pointer.update(body)
    return query

def metric(agg_type, field):
    
    # build the metric agg query body
    query_body={}
    query_body["aggs"]={
        agg_type: {
        agg_type: {
        "field": field
        }
        }
    }
    return query_body

def bucket(agg_type, agg_parameters):

    # build the bucket agg query body
    return agg_type(agg_parameters)


def query_transform(star_query):
    
    query_body={}

    # add aggregation with discrete fields to query_body

    for field in star_query["dimensions"]["discrete_fields"]:
        query_body=addSubAgg(query_body, bucket(Agg.terms, {"field":field}))

    # add aggregation with spatial field to query_body

    geo_distance_parameters={}
    geo_distance_parameters["field"]=star_query["dimensions"]["space"]["field"]
    geo_distance_parameters["origin"]=star_query["dimensions"]["space"]["origin"]
    
    distance_range=[]
    i=0
    max_radius=0
    radius=star_query["dimensions"]["space"]["radius"]
    while max_radius < star_query["dimensions"]["space"]["max_radius"]:
        distance_range.append({"from":i*radius, "to": (i+1)*radius})
        i=i+1
        max_radius=i*radius
    geo_distance_parameters["ranges"]=distance_range

    query_body=addSubAgg(query_body, bucket(Agg.geoDistance, geo_distance_parameters))
    
    # add aggregation with temporal field to query_body

    date_range_parameters={}
    date_range_parameters["field"]=star_query["dimensions"]["time"]["field"]
    
    time_ranges=[]
    current_date=datetime.strptime(star_query["dimensions"]["time"]["start"], '%d/%m/%y')
    end_date=datetime.strptime(star_query["dimensions"]["time"]["end"], '%d/%m/%y')
    while current_date < end_date:

        if star_query["dimensions"]["time"]["step"]=="day":
            time_ranges.append({"from":current_date, "to":current_date+timedelta(days=1)})
            current_date=current_date+timedelta(days=1)

        if star_query["dimensions"]["time"]["step"]=="week":
            time_ranges.append({"from":current_date, "to":current_date+timedelta(days=1)})
            current_date=current_date+timedelta(week=1)

        if star_query["dimensions"]["time"]["step"]=="month":
            time_ranges.append({"from":current_date, "to":current_date+timedelta(days=1)})
            current_date=current_date+timedelta(month=1)

        if star_query["dimensions"]["time"]["step"]=="year":
            time_ranges.append({"from":current_date, "to":current_date+timedelta(days=1)})
            current_date=current_date+timedelta(year=1)
    
    date_range_parameters["ranges"]=time_ranges

    query_body=addSubAgg(query_body, bucket(Agg.dateRange, date_range_parameters))

    # Finally add the metric agg related to the fact

    query_body=addSubAgg(query_body, metric(star_query["fact"]["agg_operation"], star_query["fact"]["field"]))

    return query_body