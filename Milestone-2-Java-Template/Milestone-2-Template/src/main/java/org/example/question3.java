package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class question3 {
    public static void solution(SparkSession spark, JavaRDD<String> eventsRDD) {
        // Convert the eventsRDD into a PairRDD with seriesid as key and the rest as value
        JavaPairRDD<Long, Tuple2<Long, Long>> eventsPairRDD = eventsRDD.mapToPair(
                (PairFunction<String, Long, Tuple2<Long, Long>>) s -> {
                    String[] parts = s.split(",");
                    Long seriesid = Long.parseLong(parts[0]);
                    Long timestamp = Long.parseLong(parts[1]);
                    Long eventid = Long.parseLong(parts[2]);
                    return new Tuple2<>(seriesid, new Tuple2<>(timestamp, eventid));
                });

        // Group by series ID
        JavaPairRDD<Long, Iterable<Tuple2<Long, Long>>> groupedEvents = eventsPairRDD.groupByKey();

        // Sort each group by timestamp and then map each group to its sorted list
        JavaPairRDD<Long, List<Tuple2<Long, Long>>> sortedGroupedEvents = groupedEvents.mapValues(
                (Iterable<Tuple2<Long, Long>> s) -> {
                    List<Tuple2<Long, Long>> sortedList = new ArrayList<>();
                    s.forEach(sortedList::add);
                    sortedList.sort(Comparator.comparing(Tuple2::_1));
                    return sortedList;
                });

        // Generate and count sequences within each series, filtering by the lambda value (count >= 5)
        JavaRDD<Tuple2<List<Long>, Integer>> sequences = sortedGroupedEvents.flatMap(
                (FlatMapFunction<Tuple2<Long, List<Tuple2<Long, Long>>>, Tuple2<List<Long>, Integer>>) seriesEntry -> {
                    Map<List<Long>, Integer> sequenceCounts = new HashMap<>();
                    List<Tuple2<Long, Long>> eventsList = seriesEntry._2;
                    for (int i = 0; i <= eventsList.size() - 3; i++) {
                        List<Long> sequence = Arrays.asList(eventsList.get(i)._2, eventsList.get(i + 1)._2, eventsList.get(i + 2)._2);
                        sequenceCounts.put(sequence, sequenceCounts.getOrDefault(sequence, 0) + 1);
                    }
                    List<Tuple2<List<Long>, Integer>> filteredSequences = new ArrayList<>();
                    for (Map.Entry<List<Long>, Integer> entry : sequenceCounts.entrySet()) {
                        if (entry.getValue() >= 5) {
                            filteredSequences.add(new Tuple2<>(entry.getKey(), entry.getValue()));
                        }
                    }
                    return filteredSequences.iterator();
                });

        // Map to sequence only and remove duplicates across all series
        JavaRDD<List<Long>> distinctSequences = sequences.map(Tuple2::_1).distinct();

        int q3 = (int) distinctSequences.count();
        System.out.println(">> [q3: " + q3 + "]");
    }
}
