
sqlContext = pyspark.sql.SQLContext(sc)
actions = sqlContext.jsonFile('/data/1.json')
actions.registerTempTable("actions")
results = sqlContext.sql("SELECT user FROM actions")

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
    

only10_11 = results.map(SiteFilter([10,11]).trim_sites)

cats=only10_11.map(only_cats)

>>> cats.collect()

[(u'fa6e5a67-dedd-4644-bd49-be0e283e449c', [129, 170, 196]), (u'c0f5f2c6-607b-41e2-834b-a08d51dd55a4', [115, 161, 131, 102, 174]),




my_viewable_events = results.map(AttrFilter(2, [10,11]).trim)
 
my_query = my_viewable_events.map(AttrFilter(0, [170, 196, 129]).trim)
  
q=my_query.filter(must_have_events)
ch=q.map(cats_histo)

