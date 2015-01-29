hadoop fs -put /tmp/purchases1.txt /purchases/

sqlContext = pyspark.sql.SQLContext(sc)

from pyspark.sql import SQLContext, Row
lines = sc.textFile('/purchases/')
parts = lines.map(lambda l: l.split(' '))
purchases_input = parts.map(lambda p: Row(user=p[0], purchase_id=int(p[1])))
purchases_input.first()
purchasesSchema = sqlContext.inferSchema(purchases_input)
purchasesSchema.first()

>>> purchases.printSchema()
root
 |-- purchase_id: integer (nullable = true)
 |-- user: string (nullable = true)
 
#Row(purchase_id=18, user=u'fa6e5a67-dedd-4644-bd49-be0e283e449c')


purchasesSchema.registerTempTable('purchases')
purchases=sqlContext.sql("select user, purchase_id from purchases")
purchases.first()
# now in right order
#Row(user=u'fa6e5a67-dedd-4644-bd49-be0e283e449c', purchase_id=18)

actionsSchema = sqlContext.jsonFile('/data/')
actionsSchema.registerTempTable("actions")
actions = sqlContext.sql("select user, events from actions");

x=actions.join(purchases)
>>> x.first()


>>> A=sqlContext.sql("select user, events from actions where user='c3a50355-8768-4472-83c6-875aaf99cef9'")
>>> A.first()
# Row(user=u'c3a50355-8768-4472-83c6-875aaf99cef9', events=[Row(category=105, ns=1, site=16), Row(category=200, ns=1, site=11), Row(category=166, ns=2, site=15), Row(category=192, ns=1, site=15), Row(category=197, ns=1, site=15), Row(category=158, ns=1, site=15), Row(category=169, ns=1, site=11), Row(category=119, ns=2, site=15), Row(category=200, ns=1, site=10), Row(category=125, ns=2, site=16), Row(category=198, ns=2, site=15), Row(category=123, ns=1, site=15), Row(category=161, ns=2, site=13), Row(category=129, ns=2, site=10)])

>>> B=sqlContext.sql("select user, purchase_id from purchases where user='c3a50355-8768-4472-83c6-875aaf99cef9'")
>>> B.first()
# Row(user=u'c3a50355-8768-4472-83c6-875aaf99cef9', purchase_id=21)

A.join(B).collect()
# [(u'c3a50355-8768-4472-83c6-875aaf99cef9', 
#   ([(105, 1, 16), (200, 1, 11), (166, 2, 15), (192, 1, 15), (197, 1, 15), (158, 1, 15), (169, 1, 11), (119, 2, 15), (200, 1, 10), (125, 2, 16), (198, 2, 15), (123, 1, 15), (161, 2, 13), (129, 2, 10)], 21)), 
#   (u'c3a50355-8768-4472-83c6-875aaf99cef9', ([(105, 1, 16), (200, 1, 11), (166, 2, 15), (192, 1, 15), (197, 1, 15), (158, 1, 15), (169, 1, 11), (119, 2, 15), (200, 1, 10), (125, 2, 16), (198, 2, 15), (123, 1, 15), (161, 2, 13), (129, 2, 10)], 14))]

Huh, joining works except there's a product of them for the value




(u'c3a50355-8768-4472-83c6-875aaf99cef9',
 ([(105, 1, 16), (200, 1, 11), (166, 2, 15), (192, 1, 15), (197, 1, 15), (158, 1, 15), (169, 1, 11), (119, 2, 15), (200, 1, 10), (125, 2, 16), (198, 2, 15), (123, 1, 15), (161, 2, 13), (129, 2, 10)], 21))
 
A2=sqlContext.sql("select user, events from actions")
B2=sqlContext.sql("select user, purchase_id from purchases")
j=A2.cogroup(B2)
j.first()

(u'c3a50355-8768-4472-83c6-875aaf99cef9', (<pyspark.resultiterable.ResultIterable object at 0x1709590>, <pyspark.resultiterable.ResultIterable object at 0x1709390>))

v1, v2 = j.first()[1]

>>> type(v1)
<class 'pyspark.resultiterable.ResultIterable'>
>>> dir(v1)
['__abstractmethods__', '__class__', '__delattr__', '__dict__', '__doc__', '__format__', '__getattribute__', '__hash__', '__init__', '__iter__', '__len__', '__metaclass__', '__module__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_abc_cache', '_abc_negative_cache', '_abc_negative_cache_version', '_abc_registry', 'data', 'index', 'maxindex']
>>> v1.data
[[(105, 1, 16), (200, 1, 11), (166, 2, 15), (192, 1, 15), (197, 1, 15), (158, 1, 15), (169, 1, 11), (119, 2, 15), (200, 1, 10), (125, 2, 16), (198, 2, 15), (123, 1, 15), (161, 2, 13), (129, 2, 10)]]
>>>

>>> v2.data
[21, 14]


def purchase14_sites(rdd):
  events = rdd[1][0].data[0]
  purchase_ids = rdd[1][1].data
  if 14 in purchase_ids:
    return rdd[0], sorted(list(set([_[2] for _ in events])))
  return rdd[0], list()
  
                
rr=j.map(purchase14_sites)
rr.first()
(u'c3a50355-8768-4472-83c6-875aaf99cef9', [16, 10, 11, 13, 15])
   
   
rr=j.map(purchase14_sites).filter(must_have_events)
qq=rr.collect()
qq
# [(u'c3a50355-8768-4472-83c6-875aaf99cef9', [10, 11, 13, 15, 16]), (u'9644d9a7-61ea-44b5-9f4f-3880d500bd94', [10, 11, 13, 14, 15, 16]),

ww=rr.flatMap(lambda rdd: [(_, 1) for _ in rdd[1]])

>>> from operator import add
>>> yy=ww.reduceByKey(add)

>>> vvv=yy.collect()

>>> vvv
[(16, 19), (12, 15), (13, 21), (10, 21), (14, 18), (11, 21), (15, 21)]



