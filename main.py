from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row


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

    # Register the DataFrames as temporary views
    events_df.createOrReplaceTempView("events")
    event_types_df.createOrReplaceTempView("event_types")

    # SparkSQL to count the occurrences of the event sequence "109,145,125"
    result = spark_session.sql("""
        SELECT COUNT(*) AS count
            FROM events e1
            JOIN events e2 ON e1.timestamp + 1 = e2.timestamp AND e1.seriesid = e2.seriesid
            JOIN events e3 ON e2.timestamp + 1 = e3.timestamp AND e2.seriesid = e3.seriesid
            WHERE e1.eventid = 109 AND e2.eventid = 145 AND e3.eventid = 125
    """).collect()

    # print(result)

    # Print the result
    # print(f">>[q21: {result}]")
    
    # Use SparkSQL to count the occurrences of the event sequence "2,11,6"
    # result = spark_session.sql("""
    #     SELECT COUNT(*) AS count
    #     FROM events e1
        
    #     JOIN events e2 ON e1.timestamp +1 = e2.timestamp AND e1.seriesid = e2.seriesid
    #     JOIN events e3 ON e2.timestamp +1 = e3.timestamp AND e2.seriesid = e3.seriesid
                               
    #     JOIN event_types et1 ON e1.eventid = et1.eventid
    #     JOIN event_types et2 ON e2.eventid = et2.eventid
    #     JOIN event_types et3 ON e3.eventid = et3.eventid
                               
    #     WHERE et1.eventtypeid = 2 AND et2.eventtypeid = 11 AND et3.eventtypeid = 6
    # """).collect()

    # result = spark_session.sql("""
    #     SELECT eventid, timestamp
    #         FROM events e1
    #         WHERE e1.eventid = 6
    # """).collect()
    
    # Convert the result into a DataFrame
    result_df = spark_session.createDataFrame(result)

    # Show the DataFrame
    result_df.show()

    # # Print the result
    # print(f">>[q22: {result}]")

    # e1.eventid, e2.eventid, e3.eventid, e1.timestamp, e2.timestamp, e3.timestamp ,et1.eventtypeid, et2.eventtypeid, et3.eventtypeid
        


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

    # q3(spark_context, cleaned_events, cleaned_event_types)

    # q4(spark_context, cleaned_events, cleaned_event_types)

    spark_context.stop()
