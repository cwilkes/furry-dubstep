
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


actionsSchema = sqlContext.jsonFile('/data/')
actionsSchema.registerTempTable("actions")
actions = sqlContext.sql("select user, events from actions");


import rads_filters

x=rads_filters.include_sites(actions, [10,12])
