package edu.upf;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.github.tukaaa.MastodonDStream;
import com.github.tukaaa.config.AppConfig;
import com.github.tukaaa.model.SimplifiedTweetWithHashtags;

import edu.upf.util.LanguageMapUtils;

public class MastodonStateless {
        public static void main(String[] args) {
                String input = args[0];

                SparkConf conf = new SparkConf().setAppName("Real-time Twitter Stateless Exercise");
                AppConfig appConfig = AppConfig.getConfig();

                StreamingContext sc = new StreamingContext(conf, Durations.seconds(10));
                JavaStreamingContext jsc = new JavaStreamingContext(sc);
                JavaRDD<String> lines = jsc.sparkContext().textFile(input);
                jsc.checkpoint("/tmp/checkpoint");

                JavaDStream<SimplifiedTweetWithHashtags> stream = new MastodonDStream(sc, appConfig).asJStream();

                // TODO IMPLEMENT ME
                JavaPairRDD<String, String> staticrdd = LanguageMapUtils.buildLanguageMap(lines);
                stream.mapToPair(tweet -> new Tuple2<String, Integer> (tweet.getLanguage(), 1)).reduceByKey((x,y) -> x+y)
                .transformToPair(rdd->rdd.join(staticrdd).mapToPair(tuple -> new Tuple2 <> (tuple._2._1, tuple._2._2))
                .sortByKey(false)).mapToPair(x -> x.swap()).print();

                // Start the application and wait for termination signal
                sc.start();
                sc.awaitTermination();
        }
}
