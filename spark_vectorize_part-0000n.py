import datetime
import pprint
epoch = datetime.datetime.utcfromtimestamp(0)

def _tokenize(line):
    """Get the file name and access timestamp from the audit log line.
    Args:
        line: line from the HDFS Audit log
    Returns:
        a tuple of file name and access time in epoch with milliseconds
    """
    def unix_time_millis(dt):
        """Given a datetime object, return the epoch with milliseconds.
        Args:
            dt: datetime object
        Returns:
            13 digit epoch with milliseconds
        """
        return (dt - epoch).total_seconds() * 1000.0
    fields = line.split()
    src = fields[7].split('=')[1]
    dst = fields[8].split('=')[1]
    cmd = fields[6].split('=')[1]
    access_t = ' '.join(fields[0:2])
    #dt = [int(unix_time_millis(datetime.datetime.strptime(' '.join(fields[0:2]), '%Y-%m-%d %H:%M:%S,%f')))]
    dt = int(unix_time_millis(datetime.datetime.strptime(' '.join(fields[0:2]), '%Y-%m-%d %H:%M:%S,%f')))
    return src, dt, cmd, dst

def return_ts_only(item):
    return item[0]

tf_rdd = sc.textFile('/Users/rohithvsm/Downloads/sample_10_mod')
col_rdd = tf_rdd.map(lambda line: _tokenize(line))
tf_rdd.unpersist()
#ts_rdd = col_rdd.filter(lambda rec: rec[2] not in ['listStatus', 'mkdirs', 'setPermission', 'setReplication', 'rename']).map(lambda rec: (rec[0], rec[1])).reduceByKey(lambda a, b: a + b)
ts_rdd = col_rdd.filter(lambda rec: rec[2] not in ['listStatus', 'mkdirs', 'setPermission', 'setReplication', 'rename']).map(lambda rec: (rec[0], [(rec[1], rec[2])])).reduceByKey(lambda a, b: a + b if b[0][-1] != 'delete' else []).map(lambda rec: (rec[0], map(return_ts_only, rec[1])))
rename_rdd = col_rdd.filter(lambda rec: rec[2] == 'rename').map(lambda rec: (rec[0], rec[3]))
col_rdd.unpersist()
pprint.pprint(rename_rdd.collect())
pprint.pprint(ts_rdd.collect())
print ts_rdd.id
for rename in rename_rdd.collect():
    pprint.pprint(rename)
    print ts_rdd.id
    ts_rdd = ts_rdd.map(lambda rec: (rename[1], rec[1]) if rec[0] == rename[0] else rec).reduceByKey(lambda a, b: a+b)
    print ts_rdd.id

# trying recursive since for loop not working with reduceByKey due to RDD partitions
renames = rename_rdd.collect()
n = len(renames)
def do_rename(old_rdd, idx):
    while idx 
    new_rdd = old_rdd.map(lambda rec: (rename[1], rec[1]) if rec[0] == rename[0] else rec)
    print old_rdd.id
    pprint.pprint(old_rdd.collect())
    print new_rdd.id
    pprint.pprint(new_rdd.collect())
    old_rdd.unpersist()
    return new_rdd
