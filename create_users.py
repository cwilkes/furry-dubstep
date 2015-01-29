import sys
import random
import uuid
import simplejson as json


class ArgParser(object):
    def __init__(self, args, start_pos=1):
        self.args = args
        self.pos = start_pos

    def next_int(self):
        ret = int(self.args[self.pos])
        self.pos += 1
        return ret

    def next_ints(self, count):
        return [self.next_int() for _ in range(count)]


def rand_maker(min_val, max_val):
    def create():
        return random.randint(min_val, max_val+1)
    return create


def event_maker(number_events, range_ns, range_sites, range_cats):
    def make_event():
        return dict(ns=range_ns(), site=range_sites(), category=range_cats())
    def all_event():
        return [make_event() for _ in range(number_events())]
    return all_event


def main(args):
    ap = ArgParser(args)
    number_users = ap.next_int()
    ranges = [rand_maker(ap.next_int(), ap.next_int()) for _ in range(4)]
    em = event_maker(*ranges)
    e2 = rand_maker(1,20)
    for _ in range(number_users):
        user = dict(user=str(uuid.uuid4()), events=em(), e2=[e2() for _ in range(4)])
        print json.dumps(user)



if __name__ == '__main__':
    sys.exit(main(sys.argv))
