package org.example;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.*;
import scala.Tuple2;


public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getRootLogger().setLevel(Level.OFF);

        boolean onServer = false; // TODO: Set this to true if and only if building a JAR to run on the server

        SparkConf conf = new SparkConf()
                .setAppName(Main.class.getName());
        if (!onServer) conf = conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .appName("2ID70")
                .getOrCreate();

        String eventsPath =  "C:/Users/20202983/2ID70/Milestone-2/2ID70-group23-milestone2/Milestone-2-Java-Template/Milestone-2-Template/src/main/java/org/example/events.csv";
        String eventTypesPath =  "C:/Users/20202983/2ID70/Milestone-2/2ID70-group23-milestone2/Milestone-2-Java-Template/Milestone-2-Template/src/main/java/org/example/event_types.csv";
        JavaRDD<String> eventsRDD = sc.textFile(eventsPath); // Todo: Load the data from the file at eventsPath
        JavaRDD<String> eventTypesRDD = sc.textFile(eventTypesPath); // Todo: Load the data from the file at eventTypesPath

        Tuple2<JavaRDD<String>, JavaRDD<String>> cleaned = question1.solution(spark, eventsRDD, eventTypesRDD);
        JavaRDD<String> df1 = cleaned._1();
        JavaRDD<String> df2 = cleaned._2();
        question2.solution(spark, df1, df2);
        question3.solution(spark, df1);
        // question4.solution(spark, df1, df2);
    }
}