$ curl -o /tmp/people1.json http://172.16.18.146:8080/people1.json
$ hadoop fs -mkdir /data/
$ hadoop fs -put /tmp/people1.json /data/1.json
$ cd /usr/local/spark
$ bin/pyspark

>>> sqlContext = pyspark.sql.SQLContext(sc)
>>> actions = sqlContext.jsonFile('/data/1.json')

>>> actions.printSchema()
root
 |-- events: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- category: integer (nullable = true)
 |    |    |-- ns: integer (nullable = true)
 |    |    |-- site: integer (nullable = true)
 |-- user: string (nullable = true)

>>> actions.registerTempTable("actions")

>>> results = sqlContext.sql("SELECT user FROM actions")




>>> results = sqlContext.sql("SELECT user, events[0].category FROM actions")
>>> results.first()
Row(user=u'015f5771-afe6-4cac-8318-a2b54ac326bd', c1=176)



>>> results = sqlContext.sql("SELECT * FROM actions where events[0].category = 176")

results.first()

Row(events=[Row(category=176, ns=1, site=11), Row(category=148, ns=2, site=13), Row(category=152, ns=2, site=14), Row(category=180, ns=2, site=11), Row(category=136, ns=1, site=15), Row(category=188, ns=2, site=13), Row(category=193, ns=1, site=10), Row(category=142, ns=1, site=13), Row(category=181, ns=1, site=15), Row(category=127, ns=2, site=15), Row(category=108, ns=1, site=16), Row(category=128, ns=1, site=13)], user=u'015f5771-afe6-4cac-8318-a2b54ac326bd')


unfortunately can't do "events.category"  bah, have to specify the position!?



results = sqlContext.sql("SELECT user, events FROM actions")
cat106=results.filter(lambda a: 106 in (_[0] for _ in a[1]))
cat106.first()

Row(user=u'fa6e5a67-dedd-4644-bd49-be0e283e449c', events=[Row(category=122, ns=1, site=12), Row(category=170, ns=1, site=10), Row(category=152, ns=2, site=13), Row(category=121, ns=1, site=16), Row(category=123, ns=1, site=15), Row(category=129, ns=2, site=10), Row(category=155, ns=2, site=13), Row(category=106, ns=1, site=16), Row(category=196, ns=2, site=10), Row(category=128, ns=2, site=14)])

cat106.count()
9

def mapper(a):
...     return [(_[0], 1) for _ in a[1]]
...
>>> all_cats = cat106.flatMap(mapper).reduceByKey(lambda x, y: x+y)

all_cats.collect()
[(128, 2), (130, 3), (132, 2), (134, 3), (136, 3), (142, 1), (146, 1), (148, 1), (152, 1), (160, 1), (164, 1), (166, 1), (168, 1), (170, 2), (174, 2), (176, 2), (178, 2), (180, 3), (182, 1), (184, 1), (186, 1), (188, 1), (190, 1), (192, 1), (194, 1), (196, 3), (198, 1), (102, 3), (106, 9), (108, 3), (110, 1), (118, 2), (122, 1), (124, 1), (126, 3), (129, 1), (137, 3), (139, 1), (141, 2), (143....


>>> ac2=all_cats.filter(lambda cc: cc[1] >= 3)
>>> ac2.collect()

[(130, 3), (134, 3), (136, 3), (180, 3), (196, 3), (102, 3), (106, 9), (108, 3), (126, 3), (137, 3), (159, 3), (169, 3), (179, 3)]

ac3 = ac2.sortByKey()
ac3.collect()

[(102, 3), (106, 9), (108, 3), (126, 3), (130, 3), (134, 3), (136, 3), (137, 3), (159, 3), (169, 3), (179, 3), (180, 3), (196, 3)]

# as 2 is the site
>>> my_viewable_cats = results.filter(lambda a: set([_[2] for _ in a[1]]).intersection([10, 11, 12]))

# 0: ns
# 1: category
# 2: site
def get_sites(a):
    return set([_[2] for _ in a[1]])
    
my_viewable_cats = results.filter(lambda a: get_sites(a).intersection([10, 11, 12]))

my_viewable_cats.first()
Row(user=u'fa6e5a67-dedd-4644-bd49-be0e283e449c', events=[Row(category=122, ns=1, site=12), Row(category=170, ns=1, site=10), Row(category=152, ns=2, site=13), Row(category=121, ns=1, site=16), Row(category=123, ns=1, site=15), Row(category=129, ns=2, site=10), Row(category=155, ns=2, site=13), Row(category=106, ns=1, site=16), Row(category=196, ns=2, site=10), Row(category=128, ns=2, site=14)])

Row(category=107, ns=1, site=13),


notice that these are only sites that the user can see, this can be cached


def get_cats(a):
    return set([_[0] for _ in a[1]])
    
    >>> cat122_and_152 = my_viewable_cats.filter(lambda a: len(get_cats(a).intersection([122,152])) == 2)
    
    [Row(user=u'fa6e5a67-dedd-4644-bd49-be0e283e449c', events=[Row(category=122, ns=1, site=12), Row(category=170, ns=1, site=10), Row(category=152, ns=2, site=13), Row(category=121, ns=1, site=16), Row(category=123, ns=1, site=15), Row(category=129, ns=2, site=10), Row(category=155, ns=2, site=13), Row(category=106, ns=1, site=16), Row(category=196, ns=2, site=10), Row(category=128, ns=2, site=14)]),
     Row(user=u'3973f3da-51b5-49ad-b444-8e0253e71aee', events=[Row(category=152, ns=1, site=14), Row(category=159, ns=1, site=14), Row(category=186, ns=2, site=14), Row(category=170, ns=2, site=13), Row(category=122, ns=1, site=16), Row(category=136, ns=1, site=12), Row(category=166, ns=1, site=13), Row(category=175, ns=2, site=10), Row(category=157, ns=1, site=13), Row(category=169, ns=1, site=12), Row(category=158, ns=1, site=16), Row(category=154, ns=1, site=11), Row(category=111, ns=1, site=12), Row(category=194, ns=1, site=16), Row(category=137, ns=2, site=14)])]
    
    
