package edu.upf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.github.tukaaa.MastodonDStream;
import com.github.tukaaa.config.AppConfig;
import com.github.tukaaa.model.SimplifiedTweetWithHashtags;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class MastodonWithState {
    public static void main(String[] args) throws InterruptedException {
        //Get language from input
        List<String> argsList = Arrays.asList(args);
        if (argsList.isEmpty()) {
            System.out.println("Please provide the necessary input parameter: language");
            System.exit(0);
        }
        String language = argsList.get(0);
        System.out.println("Starting application with language: " + language);

        //start streaming application (provided code)
        SparkConf conf = new SparkConf().setAppName("Real-time Mastodon With State");
        AppConfig appConfig = AppConfig.getConfig();

        StreamingContext sc = new StreamingContext(conf, Durations.seconds(10));
        JavaStreamingContext jsc = new JavaStreamingContext(sc);
        jsc.checkpoint("/tmp/checkpoint");

        JavaDStream<SimplifiedTweetWithHashtags> stream = new MastodonDStream(sc, appConfig).asJStream();

        //filter the usable data
        JavaDStream<SimplifiedTweetWithHashtags> languageStream = stream
                .filter(toot -> { //filter ONLY tweets of specified language
                    String lang = toot.getLanguage();
                    return !(lang==null) && lang.equals(language);
                });

        //count number of toots by each user during time
        JavaPairDStream<String, Integer> usrTootCount = languageStream
                .mapToPair(toot -> new Tuple2<>(toot.getUserName(), 1))
                .filter(pair -> !pair._1.isBlank()) //filter blank username
                .updateStateByKey((values, state) -> {
                    Integer tootCount = state.orElse(0);
                    for(Integer value : values){ //not flattened, iterate over value list
                        tootCount += value;
                    }
                    return Optional.of(tootCount);
                });

        //sort by counts (descending)
        JavaPairDStream<Integer, String> sortedTootCount = usrTootCount
                .mapToPair(Tuple2::swap)
                .transformToPair(rdd -> rdd.sortByKey(false));

        //display 20 users with most toots
        sortedTootCount.print(20);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }

}