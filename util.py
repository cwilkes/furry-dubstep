from pyspark.sql import SQLContext, Row
import simplejson as json


# position 0 is category, 1 is ns, 2 is site
# which sort of seems arbitrary
# results.map(AttrFilter(2, [10,11,12]))   will return events only with siteids of 10,11,12
class AttrFilter(object):
    def __init__(self, pos, acceptable):
        self.pos = pos
        self.acceptable = acceptable
    def trim(self, rdd):
        return rdd[0], filter(lambda _: _[self.pos] in self.acceptable, rdd[1])


# use only_cats directly
class EventEmitter(object):
    def __init__(self, pos):
        self.pos = pos
    def trim(self, rdd):
        return rdd[0], map(lambda _: _[self.pos], rdd[1])


# results.filter(must_have_events)
def must_have_events(rdd):
    return len(rdd[1])


# results.map(only_cats)
def only_cats(rdd):
    return rdd[0], list(set([_[0] for _ in rdd[1]]))


# fails with AttributeError: 'tuple' object has no attribute 'category'
#def only_cats2(rdd):
#    return rdd[0], list(set([_.category for _ in rdd[1]]))



# results.map(cats_histo) [('1x196', 2), ('1x170', 1),]
def cats_histo(rdd):
    cats = sorted(['%dx%d' % (_[1], _[0]) for _ in rdd[1]])
    return '_'.join(cats), 1

