
import rads_model


class SiteFilter(object):
    def __init__(self, acceptable_sites, userid_pos, attr_pos, site_pos):
        self.acceptable_sites = set(acceptable_sites)
        self.userid_pos = userid_pos
        self.attr_pos = attr_pos
        self.site_pos = site_pos

    def evaluate(self, rdd):
        user_id = rdd[self.userid_pos]
        attributes = list()
        for user_attr in rdd[self.attr_pos]:
            site = user_attr[self.site_pos]
            if site is None or site in self.acceptable_sites:
                attributes.append(user_attr)
        return user_id, attributes


def include_sites(acceptable_sites, userid_pos=0, attr_pos=1, site_pos=2):
    return SiteFilter(acceptable_sites, userid_pos, attr_pos, site_pos).evaluate


class QueryFilter(object):
    def __init__(self, query_node, attribute_pos):
        self.query_node = query_node
        self.attribute_pos = attribute_pos

    def evaluate(self, rdd):
        return self.query_node.evaluate(rdd[self.attribute_pos])


def apply_query(query_node, attribute_pos=1):
    return QueryFilter(rads_model.QueryNode(query_node), attribute_pos).evaluate