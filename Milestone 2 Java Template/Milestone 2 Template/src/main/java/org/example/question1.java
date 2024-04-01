package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class question1 {
    public static Tuple2<JavaRDD<String>, JavaRDD<String>> solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventTypesRDD) {
        // Filter and clean events RDD
        JavaRDD<String> cleanedEventsRDD = eventsRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                String[] parts = s.split(",");
                return parts.length == 3 && allDigits(parts);
            }
        }).map(new Function<String, String>() {
            @Override
            public String call(String s) {
                String[] parts = s.split(",");
                return String.join(",", parts); // This map is actually redundant in this context since we are not changing anything
            }
        });

        // Filter and clean event types RDD
        JavaRDD<String> cleanedEventTypesRDD = eventTypesRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                String[] parts = s.split(",");
                return parts.length == 2 && allDigits(parts);
            }
        }).map(new Function<String, String>() {
            @Override
            public String call(String s) {
                String[] parts = s.split(",");
                return String.join(",", parts); // This map is also redundant
            }
        });

        // Counters
        long eventCount = eventsRDD.count();
        long eventTypeCount = eventTypesRDD.count();
        long cleanedEventCount = cleanedEventsRDD.count();
        long cleanedEventTypeCount = cleanedEventTypesRDD.count();

        System.out.println(">> [q11: " + cleanedEventCount + "]");
        System.out.println(">> [q12: " + cleanedEventTypeCount + "]");
        System.out.println(">> [q13: " + (eventCount - cleanedEventCount) + "]");
        System.out.println(">> [q14: " + (eventTypeCount - cleanedEventTypeCount) + "]");

        return new Tuple2<>(cleanedEventsRDD, cleanedEventTypesRDD);
    }

    private static boolean allDigits(String[] parts) {
        for (String part : parts) {
            if (!part.chars().allMatch(Character::isDigit)) {
                return false;
            }
        }
        return true;
    }
}
