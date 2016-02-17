"""Extracts access time for each file from the
data file.

created on: 31st Oct., 2015
author: Rashmi Balasubramanyam <rashmi.bmanyam@gmail.com>

"""

import argparse

import sys

data = {}
iternum = 0
with open(
        '../data/train_raw.csv') as fp:  # check the type of the input file, if it is a csv or other format
    for l in fp:
        iternum += 1
        p = l.strip().split('\t')
        try:
            p[1] = ' '.join(p[1].split(','))
            filename = p[7].split('=')[1]
            if p[6] == 'cmd=rename':
                temp = data[filename]
                temp.append(' '.join(p[0:2]))
                del data[filename]  # delete old reference
                newfilename = p[8].split('=')[1]
                data[
                    newfilename] = temp  # create reference with renamed filename
                continue
            # if it not a rename command
            if not filename in data:
                data[filename] = []
            data[filename].append(' '.join(p[0:2]))
        except Exception as e:
            print(iternum)
            print(str(e))
            # raise e
            continue

with open('../data/train_timestamps_1.csv', 'w') as ofp:
    for k, v in data.items():
        ofp.write(','.join([k] + v) + '\n')

# abandoned in between. Modularizing by writing functions
# Above code written by Rashmi
def vectorize_hdfs_audit_log(audit_log):
    """Vectorize the input file containing HDFS audit log.

    Args:
        audit_log: path to the HDFS audit log file

    """

    with open(audit_log) as al:
        for line in al:
            fields = line.split()
            filename = fields[7].split('=')[1]


def parse_cmd_line_opts(argv):
    """Parse command line options and arguments.

    Args:
        argv: command-line arguments

    Returns:
        path to input file

    """

    parser = argparse.ArgumentParser(description=('Vectorize HDFS audit logs'))

    parser.add_argument('input_file', type=argparse.FileType(),
                        help='path to input file containing HDFS audit logs')

    args = parser.parse_args()

    return args.input_file


def main():
    """Parse command-line arguments and vectorize the input file."""

    input_file = parse_cmd_line_opts(sys.argv)

    vectorize_hdfs_audit_log(input_file)


if __name__ == '__main__':
    main()
