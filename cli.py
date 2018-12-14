import argparse
from convert_takeout import get_all_records

engine = argparse.ArgumentParser()
parsers = engine.add_subparsers(title='Statistics',
                                help='Generates basic statistics from data in '
                                     'located watch-history.html file(s)')

stat_p = parsers.add_parser('stats')
stat_p.set_defaults(func=get_all_records)

stat_p.add_argument('--dir',
                    help='directory with the watch-history.html file(s)')
stat_p.add_argument('-i', '--in-place', default=False, dest='write_changes',
                    help='Trim unnecessary HTML from the found files for '
                         'faster processing next time (no data is lost)')

if __name__ == '__main__':
    args = engine.parse_args()
    args.func(args.dir, args.write_changes)
