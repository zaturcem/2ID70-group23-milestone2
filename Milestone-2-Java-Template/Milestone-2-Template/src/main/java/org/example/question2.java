package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import java.util.Arrays;

public class question2 {
    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventTypesRDD) {
        // Convert JavaRDD<String> to Dataset<Row> for events
        JavaRDD<Row> eventsRowRDD = eventsRDD.map(line -> {
            String[] parts = line.split(",");
            return RowFactory.create(Long.valueOf(parts[0]), Long.valueOf(parts[1]), Long.valueOf(parts[2]));
        });
        StructType eventsSchema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("seriesid", DataTypes.LongType, false),
            DataTypes.createStructField("timestamp", DataTypes.LongType, false),
            DataTypes.createStructField("eventid", DataTypes.LongType, false)
        });
        Dataset<Row> eventsDF = spark.createDataFrame(eventsRowRDD, eventsSchema);

        // Convert JavaRDD<String> to Dataset<Row> for event types
        JavaRDD<Row> eventTypesRowRDD = eventTypesRDD.map(line -> {
            String[] parts = line.split(",");
            return RowFactory.create(Long.valueOf(parts[0]), Long.valueOf(parts[1]));
        });
        StructType eventTypesSchema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("eventid", DataTypes.LongType, false),
            DataTypes.createStructField("eventtypeid", DataTypes.LongType, false)
        });
        Dataset<Row> eventTypesDF = spark.createDataFrame(eventTypesRowRDD, eventTypesSchema);

        // Cache the events DataFrame since it's used multiple times
        eventsDF.cache();

        // Register the DataFrames as temporary views for SQL queries
        eventsDF.createOrReplaceTempView("events");
        eventTypesDF.createOrReplaceTempView("event_types");

        // SparkSQL to count the occurrences of the event sequence "109,145,125"
        long q21 = spark.sql(
            "SELECT COUNT(*) AS count " +
            "FROM events e1 " +
            "JOIN events e2 ON e1.timestamp + 1 = e2.timestamp AND e1.seriesid = e2.seriesid " +
            "JOIN events e3 ON e2.timestamp + 1 = e3.timestamp AND e2.seriesid = e3.seriesid " +
            "WHERE e1.eventid = 109 AND e2.eventid = 145 AND e3.eventid = 125"
        ).collectAsList().get(0).getLong(0);

        System.out.println(">> [q21: " + q21 + "]");

        // SparkSQL to count the occurrences of the event sequence "2,11,6" based on event types
        long q22 = spark.sql(
            "SELECT COUNT(*) AS count " +
            "FROM events e1 " +
            "JOIN events e2 ON e1.timestamp + 1 = e2.timestamp AND e1.seriesid = e2.seriesid " +
            "JOIN events e3 ON e2.timestamp + 1 = e3.timestamp AND e2.seriesid = e3.seriesid " +
            "JOIN event_types et1 ON e1.eventid = et1.eventid " +
            "JOIN event_types et2 ON e2.eventid = et2.eventid " +
            "JOIN event_types et3 ON e3.eventid = et3.eventid " +
            "WHERE et1.eventtypeid = 2 AND et2.eventtypeid = 11 AND et3.eventtypeid = 6"
        ).collectAsList().get(0).getLong(0);

        System.out.println(">> [q22: " + q22 + "]");
    }
}
