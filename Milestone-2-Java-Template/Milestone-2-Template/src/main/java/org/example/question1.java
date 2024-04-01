package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class question1 {
    public static Tuple2<JavaRDD<String>, JavaRDD<String>> solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventTypesRDD) {
        // Filtering and cleaning eventsRDD based on conditions
        JavaRDD<String> cleanedEventsRDD = eventsRDD.filter(
                line -> {
                    String[] parts = line.split(",");
                    return parts.length == 3 && allDigits(parts);
                }
        );

        // Filtering and cleaning eventTypesRDD based on conditions
        JavaRDD<String> cleanedEventTypesRDD = eventTypesRDD.filter(
                line -> {
                    String[] parts = line.split(",");
                    return parts.length == 2 && allDigits(parts);
                }
        );

        // Counting for logging purposes
        long eventCount = eventsRDD.count();
        long eventTypeCount = eventTypesRDD.count();
        long eventFilteredCount = cleanedEventsRDD.count();
        long eventTypeFilteredCount = cleanedEventTypesRDD.count();

        System.out.println(">> [q11: " + eventFilteredCount + "]");
        System.out.println(">> [q12: " + eventTypeFilteredCount + "]");
        System.out.println(">> [q13: " + (eventCount - eventFilteredCount) + "]");
        System.out.println(">> [q14: " + (eventTypeCount - eventTypeFilteredCount) + "]");

        return new Tuple2<>(cleanedEventsRDD, cleanedEventTypesRDD);
    }

    private static boolean allDigits(String[] parts) {
        for (String part : parts) {
            if (!part.matches("\\d+")) {
                return false;
            }
        }
        return true;
    }
}
