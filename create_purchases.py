import sys
import random
import uuid
import simplejson as json


class ArgParser(object):
    def __init__(self, args, start_pos=1):
        self.args = args
        self.pos = start_pos

    def next_int(self):
        return int(self.next_str())

    def next_str(self):
        ret = self.args[self.pos]
        self.pos += 1
        return ret

    def next_ints(self, count):
        return [self.next_int() for _ in range(count)]


def rand_maker(min_val, max_val):
    def create():
        return random.randint(min_val, max_val+1)
    return create

def get_user_ids(reader):
    for line in (_.strip() for _ in reader):
        yield json.loads(line)['user']


def main(args):
    ap = ArgParser(args)

    user_ids = list(get_user_ids(open(ap.next_str())))
    number_purchases_range = rand_maker(ap.next_int(), ap.next_int())
    purchase_id_range = rand_maker(ap.next_int(), ap.next_int())

    for user_id in user_ids:
        for purchase in range(number_purchases_range()):
            print user_id, purchase_id_range()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
