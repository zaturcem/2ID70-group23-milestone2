from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time

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


def q2(spark_context: SparkContext, events: list, event_types: list):
    spark_session = SparkSession(spark_context)
    # TODO: Implement Q2 here
    # Split each element in the events list by commas and create a Row object
    # rows = [Row(*event.split(',')) for event in events]

    # # Create a DataFrame from the rows
    # df_events = spark_session.createDataFrame(rows)

    # # Show the DataFrame
    # df_events.show()

    #----------- 
    start_time = time.time()
    events_rdd = spark_context.parallelize(events, numSlices=20)
    event_types_rdd = spark_context.parallelize(event_types, numSlices=20)

    # Convert each string in the RDDs into a list of integers
    events_rdd = events_rdd.map(lambda x: [int(y) for y in x.split(',')])

    event_types_rdd = event_types_rdd.map(lambda x: [int(y) for y in x.split(',')])


    # Define the schema of the DataFrames
    schema_events = StructType([
        StructField("seriesid", IntegerType(), True),
        StructField("timestamp", IntegerType(), True),
        StructField("eventid", IntegerType(), True)
    ])

    schema_event_types = StructType([
        StructField("eventid", IntegerType(), True),
        StructField("eventtypeid", IntegerType(), True)
    ])

    # Convert the RDDs into a DataFrames
    events_df = spark_session.createDataFrame(events_rdd, schema_events)

    event_types_df = spark_session.createDataFrame(event_types_rdd, schema_event_types)

    # Cache the events DataFrame and broadcast the event_types DataFrame
    # events_df.cache()
    # event_types_df_broadcast = broadcast(event_types_df)

    # broadcast_joined_df = events_df.join(event_types_df_broadcast, events_df["eventid"] == event_types_df_broadcast["eventid"])

    # broadcast_joined_df.createOrReplaceTempView("broadcast_joined_events_event_types")
    # events_df.createOrReplaceTempView("events")

    # result_q21 = spark_session.sql("""
    # SELECT COUNT(*) AS count
    # FROM events e1
    # JOIN events e2 ON e1.timestamp + 1 = e2.timestamp AND e1.seriesid = e2.seriesid
    # JOIN events e3 ON e2.timestamp + 1 = e3.timestamp AND e2.seriesid = e3.seriesid
    # WHERE e1.eventid = 109 AND e2.eventid = 145 AND e3.eventid = 125
    # """).collect()[0][0]

    # print(f">>[q21 optimized: {result_q21}]")

    # result = spark_session.sql("""
    # SELECT COUNT(*) AS count
    # FROM broadcast_joined_events_event_types e1
    # JOIN broadcast_joined_events_event_types e2 ON e1.timestamp + 1 = e2.timestamp AND e1.seriesid = e2.seriesid
    # JOIN broadcast_joined_events_event_types e3 ON e2.timestamp + 1 = e3.timestamp AND e2.seriesid = e3.seriesid
    # WHERE e1.eventtypeid = 2 AND e2.eventtypeid = 11 AND e3.eventtypeid = 6
    # """).collect()[0][0]

    # print(f">>[q22 optimized: {result}]")

    # elapsed_time = time.time() - start_time
    # print(f"Query execution time: {elapsed_time} seconds")

    # Register the DataFrames as temporary views

    events_df.cache()

    events_df.createOrReplaceTempView("events")
    event_types_df.createOrReplaceTempView("event_types")

    # SparkSQL to count the occurrences of the event sequence "109,145,125"
    result = spark_session.sql("""
        SELECT COUNT(*) AS count
            FROM events e1
            JOIN events e2 ON e1.timestamp + 1 = e2.timestamp AND e1.seriesid = e2.seriesid
            JOIN events e3 ON e2.timestamp + 1 = e3.timestamp AND e2.seriesid = e3.seriesid
            WHERE e1.eventid = 109 AND e2.eventid = 145 AND e3.eventid = 125
    """).collect()[0][0]

    print(f">>[q21: {result}]")
    
    # Use SparkSQL to count the occurrences of the event sequence "2,11,6"
    result = spark_session.sql("""
        SELECT COUNT(*) AS count
        FROM events e1
        
        JOIN events e2 ON e1.timestamp +1 = e2.timestamp AND e1.seriesid = e2.seriesid
        JOIN events e3 ON e2.timestamp +1 = e3.timestamp AND e2.seriesid = e3.seriesid
                               
        JOIN event_types et1 ON e1.eventid = et1.eventid
        JOIN event_types et2 ON e2.eventid = et2.eventid
        JOIN event_types et3 ON e3.eventid = et3.eventid
                               
        WHERE et1.eventtypeid = 2 AND et2.eventtypeid = 11 AND et3.eventtypeid = 6
    """).collect()[0][0]

    print(f">>[q22: {result}]")

    elapsed_time = time.time() - start_time
    print(f"Query execution time: {elapsed_time} seconds")

    # result = spark_session.sql("""
    #     SELECT eventid, timestamp
    #         FROM events e1
    #         WHERE e1.eventid = 6
    # """).collect()
    
    # result_df = spark_session.createDataFrame(result)

    # # Show the DataFrame
    # result_df.show()



    # e1.eventid, e2.eventid, e3.eventid, e1.timestamp, e2.timestamp, e3.timestamp ,et1.eventtypeid, et2.eventtypeid, et3.eventtypeid
        


    pass


def q3(spark_context: SparkContext, events: list, event_types: list):
    # TODO: Implement Q3 here
    spark_session = SparkSession(spark_context)

    events_rdd = spark_context.parallelize(events, numSlices=20)

    # Convert each string in the RDDs into a list of integers
    events_rdd = events_rdd.map(lambda x: [int(y) for y in x.split(',')])

    # # Define the schema of the DataFrames
    # schema_events = StructType([
    #     StructField("seriesid", IntegerType(), True),
    #     StructField("timestamp", IntegerType(), True),
    #     StructField("eventid", IntegerType(), True)
    # ])


    # event_RDD_series_4 = events_rdd.filter(lambda x : x[0]== 4)

    # event_RDD_sortby_4 = event_RDD_series_4.sortBy(lambda x : x[1])

    # event_df = spark_session.createDataFrame(event_RDD_sortby_4, schema=schema_events)
    # event_df.show()


    pass

def find_l_frequent_sequences_within_series(spark_context: SparkContext, events: list, lambda_value: int = 5, sequence_size: int = 3):
    # Parallelize the input events list
    events_rdd = spark_context.parallelize(events)

    # Convert each event string to a list of integers
    events_rdd = events_rdd.map(lambda event: [int(part) for part in event.split(',')])

    # Group events by the series ID and sort each group by timestamp
    events_grouped_by_series = events_rdd.groupBy(lambda event: event[0]).map(
        lambda group: (group[0], sorted(list(group[1]), key=lambda event: event[1]))
    )

    # Generate and count sequences within each series
    def generate_and_count_sequences(events):
        _, events_list = events
        sequences = {}
        for i in range(len(events_list) - sequence_size + 1):
            sequence = tuple(events_list[j][2] for j in range(i, i + sequence_size))
            if sequence in sequences:
                sequences[sequence] += 1
            else:
                sequences[sequence] = 1
        # Filter sequences by λ value within this series
        return [(sequence, count) for sequence, count in sequences.items() if count >= lambda_value]

    # FlatMap to transform and filter sequences within each series
    lambda_frequent_sequences_rdd = events_grouped_by_series.flatMap(generate_and_count_sequences)

    # Map to sequence only and remove duplicates across all series
    distinct_sequences = lambda_frequent_sequences_rdd.map(lambda sequence_count: sequence_count[0]).distinct()

    # Count the number of distinct λ-frequent sequences
    num_distinct_sequences = distinct_sequences.count()

    print(f"Number of distinct λ-frequent sequences of size {sequence_size} within series: {num_distinct_sequences}")



def q4(spark_context: SparkContext, events: RDD, event_types: RDD):
    # TODO: Implement Q4 here
    return


if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    cleaned_events, cleaned_event_types = q1(spark_context, on_server)

    # q2(spark_context, cleaned_events, cleaned_event_types)

    # q3(spark_context, cleaned_events, cleaned_event_types)

    find_l_frequent_sequences_within_series(spark_context, cleaned_events)

    # q4(spark_context, cleaned_events, cleaned_event_types)

    # spark_context.stop()
