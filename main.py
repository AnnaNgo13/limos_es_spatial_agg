from bucket_agg import *
from query import *

if __name__ == "__main__":

  # # Test 
  # # group by appName, devicename, geodistance (location), and daterange(servertimestampUTC)
  # # and compute average temperature

  # query_body=initiateQuery(bucket(Agg.terms, {"field":"applicationName"}))

  # query_body=addSubAgg(query_body, bucket(Agg.terms, {"field":"deviceName"}))

  # query_body=addSubAgg(query_body, bucket(Agg.geoDistance, {"field":"location", "origin":"45.759184,3.111920", "ranges":[{ "to": 1000 },{ "from": 1000, "to": 3000 },{ "from": 3000 }]}))

  # query_body=addSubAgg(query_body, bucket(Agg.dateRange, {"field":"servertimestampUTC", "ranges": [{ "to": "now-10M/M" },{ "from": "now-10M/M" }]}))

  # query_body=addSubAgg(query_body, metric("avg", "data-temperature"))

  # query_result=es.search(index="jardin_irstea_temp", body=query_body, size=0)

  # Test  query_transform
  # This function transforms an analysis query defined in our specified language to a query in ES query language
  # Our language allow to specify 

  star_query_body={"fact":{"field":"data-temperature","agg_operation":"min"},
    "dimensions":{"discrete_fields":["applicationName", "deviceName"],
    "time":{"field":"servertimestampUTC", "step":"day", "start":"11/11/19", "end":"13/11/19"}, 
    "space":{"field":"location", "origin":"45.759184,3.111920", "radius":1000, "max_radius":10000}
    }
  }

  es_query_body=query_transform(star_query_body)

  query_result=es.search(index="jardin_irstea_temp", body=es_query_body, size=0)

  #print(query_result)
  print(es_query_body)