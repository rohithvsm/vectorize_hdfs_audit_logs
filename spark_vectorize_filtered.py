import datetime
#import pprint

from pyspark import SparkContext, SparkConf

def _get_conf(name):
    """Set configuration parameters and return."""

    return (SparkConf().set('spark.app.name', name)
            .set('spark.master', 'spark://10.0.1.86:7077')
            .set('spark.driver.memory', '1g')
            .set('spark.eventLog.enabled', 'true')
            .set('spark.eventLog.dir',
                 '/home/ubuntu/storage/logs')
            .set('spark.executor.memory', '21g')
            .set('spark.executor.cores', '4')
            .set('spark.task.cpus', '1'))

sc = SparkContext(conf=_get_conf('Sankalpa_HDFS_Learner'))

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
    fields = line.split(',')
    src = fields[2].split('=')[1]
    dst = fields[3].split('=')[1]
    cmd = fields[1].split('=')[1]
    #dt = [int(unix_time_millis(datetime.datetime.strptime(' '.join(fields[0:2]), '%Y-%m-%d %H:%M:%S %f')))]
    dt = int(unix_time_millis(datetime.datetime.strptime(fields[0], '%Y-%m-%d %H:%M:%S %f')))
    return src, dt, cmd, dst

def return_ts_only(item):
    return item[0]

#p0_rdd = sc.textFile('sankalpa_hdfs_learner/part0_filtered.csv')
#p1_rdd = sc.textFile('sankalpa_hdfs_learner/part1_filtered.csv')
#p2_rdd = sc.textFile('sankalpa_hdfs_learner/part2_filtered.csv')
#p3_rdd = sc.textFile('sankalpa_hdfs_learner/part3_filtered.csv')
#p4_rdd = sc.textFile('sankalpa_hdfs_learner/part4_filtered.csv')
#p5_rdd = sc.textFile('sankalpa_hdfs_learner/part5_filtered.csv')
#p6_rdd = sc.textFile('sankalpa_hdfs_learner/part6_filtered.csv')
tf_rdd = sc.textFile('sankalpa_hdfs_learner/part6_filtered.csv')
#tf_rdd = p0_rdd.union(p1_rdd).union(p2_rdd).union(p3_rdd).union(p4_rdd).union(p5_rdd).union(p6_rdd)
col_rdd = tf_rdd.map(lambda line: _tokenize(line))
tf_rdd.unpersist()
ts_rdd = col_rdd.filter(lambda rec: rec[2] != 'rename').map(lambda rec: (rec[0], [(rec[1], rec[2])])).reduceByKey(lambda a, b: a + b if b[0][-1] != 'delete' else []).map(lambda rec: (rec[0], map(return_ts_only, rec[1])))

#ts_rdd = col_rdd.filter(lambda rec: rec[2] not in ['listStatus', 'mkdirs', 'setPermission', 'setReplication', 'rename']).map(lambda rec: (rec[0], rec[1])).reduceByKey(lambda a, b: a + b)

rename_rdd = col_rdd.filter(lambda rec: rec[2] == 'rename').map(lambda rec: (rec[0], rec[3]))
col_rdd.unpersist()

for rename in rename_rdd.collect():
    ts_rdd = ts_rdd.map(lambda rec: (rename[1], rec[1]) if rec[0] == rename[0] else rec).reduceByKey(lambda a, b: a+b)

ts_rdd.saveAsTextFile('trial/output')
ts_rdd.map(lambda rec: (rec[0], len(rec[1]))).saveAsTextFile('trial/output_counts')