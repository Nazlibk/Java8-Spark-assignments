import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;

public class TwitterMessagesAnalysis {
    public static void main(String[] args) throws InterruptedException {
        //Part A - question 1
        //Twitter configurations
        final String consumerKey = "tusGH6zWEyCrNyPGEHp7rZO6M";
        final String consumerSecret = "WAigSRgGlrTAGj1cOraHOtBvsoBnB3Vr7GVGrPg5KY7qQZsOMi";
        final String accessToken = "837614624442822656-830VsYGN3chlEVdlP5WUkXXLwozRglF";
        final String accessTokenSecret = "Un25ffrFk0MxkmUdWTSziIgASt2ScvfckNy5eN1ecin2J";

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        //Spark configurations
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Assignment5");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(1000));

        //Part A - question  2
        //Gets tweets from twitter.
        JavaDStream<Status> tweets = TwitterUtils.createStream(javaStreamingContext);
        //Extracts the text of each tweet.
        JavaDStream<String> tweetsText = tweets.map(status -> status.getText());
        //For each tweet, finds and print the results.
        tweetsText.foreachRDD(tweetT -> {
            tweetT.foreach(t -> {
                //Prints tweet itself.
                System.out.println("Tweet:");
                System.out.println();
                System.out.println();
                System.out.println(t);
                System.out.println();
                //Splits the words from tweet.
                String[] words = t.split(" ");
                System.out.println("Words count: " + words.length);
                //Counts the characters in tweet.
                int charCount = t.split("").length;
                System.out.println("Characters count: " + charCount);
                System.out.print("HashTags: ");
                //Extracts hashtags from tweet and prints them.
                Arrays.stream(t.split(" ")).filter(w -> w.startsWith("#")).forEach(ht -> System.out.print(ht + "\t"));
                System.out.println();
                //Calculates the average of characters for tweet.
                int sumOfCharacters = Arrays.stream(words).map(w -> w.split("").length).reduce(0, (a, b) -> a + b);
                System.out.println("Characters average: " + sumOfCharacters / (double) words.length);
                System.out.println("-------------------------------------------------------");
            });
        });


        //Part A - question 3
        //Finds and prints the average of characters for tweets in the last 5 minutes of tweets every 30 seconds.
        JavaPairDStream<String, String[]> words = tweetsText.mapToPair(tweetTxt -> new Tuple2<>(tweetTxt, tweetTxt
                .split(" ")));
        JavaPairDStream<String, Integer> eachWordsCharacters = words.mapValues(ws -> Arrays.stream(ws)
                                                                                           .map(w -> w.split("").length)
                                                                                           .reduce(0, (a, b) -> a + b));
        JavaDStream<Tuple2<String, Double>> wordsCharactersAverage = eachWordsCharacters.map(tweetTxt -> new Tuple2<>(tweetTxt._1, tweetTxt._2 / (double) tweetTxt._1
                .split(" ").length));
        wordsCharactersAverage.window(Durations.seconds(300), Durations.seconds(30))
                              .foreachRDD(wca -> wca.foreach(c -> {
                                  System.out.println("Characters average in the last 5 minutes of tweets every 30 seconds.");
                                  System.out.println("tweet: ");
                                  System.out.println(c._1);
                                  System.out.println("Characters average: " + c._2);
                                  System.out.println("***************************************");
                              }));

        //Finds and prints the 10 hashtags from tweets in the last 5 minutes of tweets every 30 seconds.
        JavaDStream<String> hashTags = tweetsText.flatMap(tweetsTxt -> Arrays.stream(tweetsTxt.split(" "))
                                                                             .filter(w -> w.startsWith("#"))
                                                                             .iterator());
        hashTags.window(Durations.seconds(300), Durations.seconds(30)).foreachRDD(s -> {
            List<String> take = s.take(10);
            System.out.println("Top 10 hashtags in the last 5 minutes of tweets every 30 seconds.");
            take.forEach(p -> System.out.println(p + "\t"));
        });

        javaStreamingContext.checkpoint("checkPoint");
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
