
from collections import namedtuple
from pyspark.sql import Row

ACCEPTABLE_OPERATIONS = set(['AND', 'OR', 'NOT'])
DEFAULT_NS_ID = 1
DEFAULT_FREQ_MIN = 1
DEFAULT_FREQ_MAX = 1023
DEFAULT_RECENCY_MIN = 0
DEFAULT_RECENCY_MAX = 29

RadsData = namedtuple('RadsData', 'ns site cat freq recency')


def make_rads(cat, site=None, ns=1, freq=None, recency=None):
    return RadsData(ns, site, cat, freq, recency)


class QueryException(Exception):
    pass


def make_rads_data(parts):
    ns_cats = list()
    for p in parts[1:-1]:
        p2 = p.split(',')
        ns_cats.append(Row(ns=int(p2[0]), site=int(p2[1]), cat=int(p2[2]), freq=int(p2[3]), recency=int(p2[4])))
    return Row(user=int(parts[0]), ns_cats=ns_cats, profile_type=int(parts[-1]))


class CategoryNode(object):
    def __init__(self, j):
        self.cat = j['cat']
        self.ns = j.get('ns', 1)
        self.freq = j.get('freq', [DEFAULT_FREQ_MIN, DEFAULT_FREQ_MAX])
        self.recency = j.get('recency', [DEFAULT_RECENCY_MIN, DEFAULT_RECENCY_MAX])

    def _ok_cat(self, user_attr):
        return self.ns == user_attr.ns and self.cat == user_attr.cat

    def _ok_freq(self, user_attr):
        if user_attr.freq is None:
            return True
        return self.freq[0] < user_attr.freq < self.freq[1]

    def _ok_recency(self, user_attr):
        if user_attr.recency is None:
            return True
        return self.recency[0] < user_attr.recency < self.recency[1]

    def evaluate(self, attributes):
        for user_attr in attributes:
            # break out for now
            stat_cat = self._ok_cat(user_attr)
            stat_freq = self._ok_freq(user_attr)
            stat_recency = self._ok_recency(user_attr)
            print 'attr %s, cat (%s,%s==%s,%s) %s freq %s recency %s' % (user_attr, stat_cat, self.ns, self.cat, user_attr.ns, user_attr.cat, stat_freq, stat_recency)
            if stat_cat and stat_freq and stat_recency:
                return True
        return False

    def __repr__(self):
        import json
        return json.dumps(dict(cat=self.cat, ns=self.ns, freq=self.freq, recency=self.recency))


class QueryNode(object):
    def __init__(self, j):
        keys = list(j.keys())
        if len(keys) != 1:
            raise QueryException('Only should have one key, not: %s' % (keys, ))
        self.my_op = keys[0]
        if self.my_op not in ACCEPTABLE_OPERATIONS:
            raise QueryException('Operation "%s" not permitted, only %s' % (self.my_op, ACCEPTABLE_OPERATIONS))
        if type(j[self.my_op]) != list:
            raise QueryException('Operation "%s" value must be a list, not %s' % (self.my_op, j[self.my_op]))
        if len(j[self.my_op]) == 0:
            raise QueryException('Operation "%s" value must have some elements' % (self.my_op, ))
        self.operands = list()
        for child in j[self.my_op]:
            if 'cat' in child:
                self.operands.append(CategoryNode(child))
            else:
                self.operands.append(QueryNode(child))

    def evaluate(self, attributes):
        # doesn't handle NOT
        for node in self.operands:
            if node.evaluate(attributes):
                # if this is an "OR" segment and any of the subqueries return True then return True immediately
                if self.my_op == 'OR':
                    return True
            else:
                # if this is an "AND" segment and any of the subqueries return False then return False immediately
                if self.my_op == 'AND':
                    return False
        if self.my_op == 'OR':
            # none of my subqueries returned True
            return False
        # this was an AND query and all the subqueries returned True
        return True

    def __repr__(self):
        return '{"%s": %s}' % (self.my_op, self.operands)