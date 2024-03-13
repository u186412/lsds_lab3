package edu.upf;

import com.github.tukaaa.MastodonDStream;
import com.github.tukaaa.config.AppConfig;
import com.github.tukaaa.model.SimplifiedTweetWithHashtags;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class MastodonStreamingExample {

    public static void main(String[] args) {

        // Sample JSON string
        String jsonString = "{\"name\": \"John\", \"age\": 30}";

        // Create a JsonParser object
        JsonParser parser = new JsonParser();

        try {
            // Parse the JSON string into a JsonElement
            JsonElement jsonElement = parser.parseString(jsonString);

            // Print the parsed JSON element
            System.out.println("Parsed JSON Element:");
            System.out.println(jsonElement);
        } catch (Exception e) {
            // Handle any parsing exceptions
            e.printStackTrace();
        }

        SparkConf conf = new SparkConf().setAppName("Real-time Mastodon Example");
        AppConfig appConfig = AppConfig.getConfig();
        StreamingContext sc = new StreamingContext(conf, Durations.seconds(10));
        // This is needed by spark to write down temporary data
        sc.checkpoint("/tmp/checkpoint");



        final JavaDStream<SimplifiedTweetWithHashtags> stream = new MastodonDStream(sc, appConfig).asJStream();

        stream.print(10);

        // Start the application and wait for termination signal
        sc.start();
        sc.awaitTermination();
    }

}
