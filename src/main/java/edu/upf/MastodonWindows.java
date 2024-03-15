package edu.upf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.github.tukaaa.MastodonDStream;
import com.github.tukaaa.config.AppConfig;
import com.github.tukaaa.model.SimplifiedTweetWithHashtags;

import edu.upf.util.LanguageMapUtils;
import scala.Tuple2;

public class MastodonWindows {
        public static void main(String[] args) {
                String input = args[0];

                SparkConf conf = new SparkConf().setAppName("Real-time Mastodon Stateful with Windows Exercise");
                AppConfig appConfig = AppConfig.getConfig();

                StreamingContext sc = new StreamingContext(conf, Durations.seconds(10));
                JavaStreamingContext jsc = new JavaStreamingContext(sc);
                jsc.checkpoint("/tmp/checkpoint");

                JavaDStream<SimplifiedTweetWithHashtags> stream = new MastodonDStream(sc, appConfig).asJStream();

                // OUR CODE:

                //Load language map from file
                JavaPairRDD<String, String> languageMap = LanguageMapUtils.buildLanguageMap(jsc.sparkContext().textFile(input));
                
                //Define a batch-based DStream of (language, count) pairs
                JavaPairDStream<String, Integer> batchCounts = stream
                        .mapToPair(tweet -> new Tuple2<>(tweet.getLanguage(), 1)) //Map pairs: (language, 1)
                        .transformToPair(rdd -> rdd.join(languageMap)) //Join language map 
                        .mapToPair(pair -> new Tuple2<>(pair._2()._2(), pair._2()._1())) 
                        .reduceByKey((a,b) -> a + b); //Count for each language
                
                //Swap the key-value pairs and sort them by key from batch counts
                JavaPairDStream<Integer, String> batchCountsSwaped = batchCounts
                        .mapToPair(Tuple2::swap) 
                        .transformToPair(rdd -> rdd.sortByKey(false)); 

                //Define a window-based DStream of (language, count)
                JavaPairDStream<String, Integer> windowCounts = stream
                        .mapToPair(tweet -> new Tuple2<>(tweet.getLanguage(), 1)) // Map each tweet to (language, 1) pair
                        .reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(60)); // Reduce by key over a window of 60 seconds to get counts for each language
                
                // Sort within the window
                JavaPairDStream<String, Integer> sortedWindowCounts = windowCounts
                        .transformToPair(rdd -> rdd.sortByKey(false));
                
                
                // Print the top 3 languages with highest counts for batch counts
                batchCountsSwaped.print(3);
                
                // Print the top 3 languages with highest counts within each window
                sortedWindowCounts.print(3);

                // Start Spark Streaming application and wait for termination signal
                sc.start(); 
                try {
                        sc.awaitTermination();
                } catch(InterruptedException e){
                        e.printStackTrace();
                }
        }

}