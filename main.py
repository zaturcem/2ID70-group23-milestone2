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
    event_types_file_path = "/eventtypes.csv" if on_server else "event_types.csv"

    # TODO: Implement Q1 here

    event_RDD = spark_context.textFile(events_file_path)
    event_type_RDD = spark_context.textFile(event_types_file_path)

    event_RDD_filtered = event_RDD.filter(
        lambda line: len(line.split(',')) == 3 and all(map(lambda x: x.isdigit(), line.split(','))))
    event_type_RDD_filtered = event_type_RDD.filter(
        lambda line: len(line.split(',')) == 2 and all(map(lambda x: x.isdigit(), line.split(','))))

    cleaned_events = event_RDD_filtered.map(lambda x: [int(y) for y in x.split(',')])
    cleaned_event_types = event_type_RDD_filtered.map(lambda x: [int(y) for y in x.split(',')])

    event_count = event_RDD.count()
    event_type_count = event_type_RDD.count()
    event_filtered_count = event_RDD_filtered.count()
    event_type_filtered_count = event_type_RDD_filtered.count()
    print(f""">>[q11: {event_filtered_count}]\n>>[q12: {event_type_filtered_count}]\n>>[q13: {event_count - event_filtered_count}]\n>>[q14: {event_type_count - event_type_filtered_count}]""")
    
    return cleaned_events, cleaned_event_types


def q2(spark_context: SparkContext, events: RDD, event_types: RDD):
    spark_session = SparkSession(spark_context)
    # TODO: Implement Q2 here

    # convert each string in the RDDs into a list of integers and then directly to DataFrames with column names
    events_df = events.toDF(["seriesid", "timestamp", "eventid"])

    event_types_df = event_types.toDF(["eventid", "eventtypeid"])

    # cache the events DataFrame since it's used multiple times in the queries
    events_df.cache()

    # register the DataFrames as temporary views for SQL queries
    events_df.createOrReplaceTempView("events")
    event_types_df.createOrReplaceTempView("event_types")

    # SparkSQL to count the occurrences of the event sequence "109,145,125"
    result_q21 = spark_session.sql("""
        SELECT COUNT(*) AS count
        FROM events e1
        JOIN events e2 ON e1.timestamp + 1 = e2.timestamp AND e1.seriesid = e2.seriesid
        JOIN events e3 ON e2.timestamp + 1 = e3.timestamp AND e2.seriesid = e3.seriesid
        WHERE e1.eventid = 109 AND e2.eventid = 145 AND e3.eventid = 125
    """).collect()[0][0]

    print(f">>[q21: {result_q21}]")

    # SparkSQL to count the occurrences of the event sequence "2,11,6"
    result_q22 = spark_session.sql("""
        SELECT COUNT(*) AS count
        FROM events e1
        JOIN events e2 ON e1.timestamp + 1 = e2.timestamp AND e1.seriesid = e2.seriesid
        JOIN events e3 ON e2.timestamp + 1 = e3.timestamp AND e2.seriesid = e3.seriesid
        JOIN event_types et1 ON e1.eventid = et1.eventid
        JOIN event_types et2 ON e2.eventid = et2.eventid
        JOIN event_types et3 ON e3.eventid = et3.eventid
        WHERE et1.eventtypeid = 2 AND et2.eventtypeid = 11 AND et3.eventtypeid = 6
    """).collect()[0][0]

    print(f">>[q22: {result_q22}]")


    pass


def q3(spark_context: SparkContext, events: RDD, event_types: RDD):
    # TODO: Implement Q3 here
    # Group events by the series ID and sort each group by timestamp
    events_grouped_by_series = events.groupBy(lambda event: event[0]).map(
        lambda group: (group[0], sorted(list(group[1]), key=lambda event: event[1]))
    )

    # Generate and count sequences within each series
    def generate_and_count_sequences(events):
        _, events_list = events
        sequences = {}
        for i in range(len(events_list) - 3 + 1):
            sequence = tuple(events_list[j][2] for j in range(i, i + 3))
            if sequence in sequences:
                sequences[sequence] += 1
            else:
                sequences[sequence] = 1
        # Filter sequences by λ value within this series
        return [(sequence, count) for sequence, count in sequences.items() if count >= 5]

    # FlatMap to transform and filter sequences within each series
    lambda_frequent_sequences_rdd = events_grouped_by_series.flatMap(generate_and_count_sequences)

    # Map to sequence only and remove duplicates across all series
    distinct_sequences = lambda_frequent_sequences_rdd.map(lambda sequence_count: sequence_count[0]).distinct()

    print(f">>[q3: {distinct_sequences.count()}]")

    pass

def q4(spark_context: SparkContext, events: RDD, event_types: RDD):
    # TODO: Implement Q4 here
    return


if __name__ == '__main__':
    on_server = True  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    cleaned_events, cleaned_event_types = q1(spark_context, on_server)

    q2(spark_context, cleaned_events, cleaned_event_types)

    q3(spark_context, cleaned_events, cleaned_event_types)

    q4(spark_context, cleaned_events, cleaned_event_types)

    spark_context.stop()
