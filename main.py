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

    cleaned_events = event_RDD.filter(
        lambda line: len(line.split(',')) == 3 and all(map(lambda x: x.isdigit(), line.split(','))))
    cleaned_event_types = event_type_RDD.filter(
        lambda line: len(line.split(',')) == 2 and all(map(lambda x: x.isdigit(), line.split(','))))

    print(f""">>[q11: {cleaned_events.count()}]\n>>[q12: {cleaned_event_types.count()}]\n>>[q13: {event_RDD.count() - cleaned_events.count()}]\n>>[q14: {event_type_RDD.count() - cleaned_event_types.count()}]""")
    
    return cleaned_events, cleaned_event_types


def q2(spark_context: SparkContext, events: RDD, event_types: RDD):
    spark_session = SparkSession(spark_context)
    # TODO: Implement Q2 here
    pass


def q3(spark_context: SparkContext, events: RDD, event_types: RDD):
    # TODO: Implement Q3 here
    def find_l_frequent_sequences_within_seriess(spark_context: SparkContext, events: list, lambda_value: int = 5,
                                                sequence_size: int = 3):

        # Convert each event string to a list of integers
        events = events.map(lambda event: [int(part) for part in event.split(',')])

        # Group events by the series ID and sort each group by timestamp
        events_grouped_by_series = events.groupBy(lambda event: event[0]).map(
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

        print(
            f"Number of distinct lambda-frequent sequences of size {sequence_size} within series: {num_distinct_sequences}")
    find_l_frequent_sequences_within_seriess(spark_context, cleaned_events)
    return


def q4(spark_context: SparkContext, events: RDD, event_types: RDD):
    # TODO: Implement Q4 here

    # Join the RDDs
    joined_RDD = events.map(lambda x: (x.split(',')[2], (x.split(',')[0], x.split(',')[1]))).join(event_types.map(lambda x: ((x.split(',')[0], x.split(',')[1]))))

    # eventid, seriesid, timestamp, eventtypeid
    RDDQ4 = joined_RDD.map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][1]))

    # sort by timestamp
    RDDQ4_time = RDDQ4.sortBy(lambda x: (x[1], x[2]))

    # group by seriesid
    RDDQ4_seriesgroup = RDDQ4_time.groupBy(lambda x: x[1])

    def find_l_frequent_type_sequences_within_series (event_types: RDD, lambda_value: int = 5, sequence_size: int = 5):
        # Generate and count sequences within each series
        def generate_and_count_sequences(event_types):
            sequences = {}

            sequence_all = []
            for line in event_types:
                sequence_all += [line[3]]


            for i in range(len(event_types) - sequence_size + 1):
                sequence = tuple(sequence_all[j] for j in range(i, i + sequence_size))
                if sequence in sequences.keys():
                    sequences[sequence] += 1
                else:
                    sequences[sequence] = 1
            # Filter sequences by λ value within this series
            return [(sequence, count) for sequence, count in sequences.items() if count >= lambda_value]

        # FlatMap to transform and filter sequences within each series
        lambda_frequent_sequences_rdd = event_types.flatMap(lambda x: generate_and_count_sequences(x[1]))

        # Map to sequence only and remove duplicates across all series
        distinct_sequences = lambda_frequent_sequences_rdd.keys().distinct()

        print(f""">> [q4: {distinct_sequences.count()}]""")
    #------------------------------------------------------------------------------------------------------------

    find_l_frequent_type_sequences_within_series(RDDQ4_seriesgroup)
    return


if __name__ == '__main__':
    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    cleaned_events, cleaned_event_types = q1(spark_context, on_server)

    q2(spark_context, cleaned_events, cleaned_event_types)

    q3(spark_context, cleaned_events, cleaned_event_types)

    q4(spark_context, cleaned_events, cleaned_event_types)

    spark_context.stop()
