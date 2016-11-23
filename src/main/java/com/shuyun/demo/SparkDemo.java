package com.shuyun.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author hewei
 * @version 5.0
 * @date 16/11/23  14:15
 * @desc
 */
public class SparkDemo {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        //JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
        //    @Override
        //    public Iterator<String> call(String x) {
        //        return Arrays.asList(x.split(" ")).iterator();
        //    }
        //});
        //JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
        //    @Override
        //    public Tuple2<String, Integer> call(String s) {
        //        return new Tuple2<>(s, 1);
        //    }
        //});
        //JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
        //    @Override
        //    public Integer call(Integer i1, Integer i2) {
        //        return i1 + i2;
        //    }
        //});

        JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

        wordCounts.print();

        jssc.start();

        jssc.awaitTermination();
    }

}
