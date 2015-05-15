from pyspark import SparkContext
import sys
import rads_filter


def process(partner_id, country_code, device):
    sc = SparkContext()
    permissions_rdd = sc.textFile('/permissions/%d/%s/%s/include.txt' % (partner_id, country_code, device))
    rads_rdd = sc.textFile('/rads-30day-agg/2015-05-11/part0/part-r-00000.gz')

    acceptable_sites = [int(_) for _ in permissions_rdd.collect()]
    filter_by_site = rads_filter.SiteFilterTsv(acceptable_sites).evaluate

    actions_with_permissions = rads_rdd.map(filter_by_site)

    double_cat_counts = actions_with_permissions.\
        flatMap(rads_filter.double_categories).\
        reduceByKey(lambda x, y: x+y)

    return double_cat_counts


def main():
    args = sys.argv[1:]
    partner_id = int(args.pop(0))
    country_code = args.pop(0)
    device = args.pop(0)


if __name__ == '__main__':
    main()