from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession


# The code below may help you if your pc cannot find the correct python executable.
# Don't use this code on the server!
# import os
# import sys
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# TODO: Make sure that if you installed Spark version 3.3.4 (recommended) that you install the same version of
#  PySpark. You can do this by running the following command: pip install pyspark==3.3.4


def get_spark_context(on_server: bool) -> SparkContext:
    spark_conf = SparkConf().setAppName("2ID70")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")

    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        #  log level to anything below WARN, Spark will print too much information.
        spark_context.setLogLevel("ERROR")

    return spark_context


def q1(spark_context: SparkContext, on_server: bool) -> (RDD, RDD):
    events_file_path = "/events.csv" if on_server else "events.csv"
    event_types_file_path = "/event_types.csv" if on_server else "event_types.csv"

    # TODO: Implement Q1 here

    event_RDD = spark_context.textFile(events_file_path)
    event_type_RDD = spark_context.textFile(event_types_file_path)

    event_RDD_filtered = event_RDD.filter(
        lambda line: len(line.split(',')) == 3 and all(map(lambda x: x.isdigit(), line.split(','))))
    event_type_RDD_filtered = event_type_RDD.filter(
        lambda line: len(line.split(',')) == 2 and all(map(lambda x: x.isdigit(), line.split(','))))

    cleaned_events = event_RDD_filtered.collect()
    cleaned_event_types = event_type_RDD_filtered.collect()

    print(f""">>[q11: {event_RDD_filtered.count()}]\n>>[q12: {event_type_RDD_filtered.count()}]\n>>[q13: {event_RDD.count() - event_RDD_filtered.count()}]\n>>[q14: {event_type_RDD.count() - event_type_RDD_filtered.count()}]""")
    
    return cleaned_events, cleaned_event_types


def q2(spark_context: SparkContext, events: RDD, event_types: RDD):
    spark_session = SparkSession(spark_context)
    # TODO: Implement Q2 here
    pass


def q3(spark_context: SparkContext, events: RDD, event_types: RDD):
    # TODO: Implement Q3 here
    pass


def q4(spark_context: SparkContext, events: RDD, event_types: RDD):
    # TODO: Implement Q4 here
    return


if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    cleaned_events, cleaned_event_types = q1(spark_context, on_server)

    q2(spark_context, cleaned_events, cleaned_event_types)

    q3(spark_context, cleaned_events, cleaned_event_types)

    q4(spark_context, cleaned_events, cleaned_event_types)

    spark_context.stop()
