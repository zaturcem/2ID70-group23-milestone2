package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class question1 {
    public static Tuple2<JavaRDD<String>, JavaRDD<String>> solution(SparkSession spark, JavaRDD<String> eventsRDD, JavaRDD<String> eventTypesRDD) {
        int q11 = 0;
        int q12 = 0;
        JavaRDD<String> cleanedEventsRDD = eventsRDD;
        JavaRDD<String> cleanedEventTypesRDD = eventTypesRDD;
        int q13 = 0;
        int q14 = 0;

        System.out.println(">> [q11: " + q11 + "]");
        System.out.println(">> [q12: " + q12 + "]");
        System.out.println(">> [q13: " + q13 + "]");
        System.out.println(">> [q14: " + q14 + "]");

        return new Tuple2<>(cleanedEventsRDD, cleanedEventTypesRDD);
    }
}
