import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;


public class KmeansSpark {

    static JavaSparkContext sc;

    public static void main(String... args) {
        //JavaSparkContext configuration
        sc = new JavaSparkContext(new SparkConf().setAppName("Assignment4").setMaster("local"));

        //Read data from twitter2D.txt file to an JavaRDD.
        JavaRDD<String> data = sc.textFile("twitter2D.txt");
        //Split each line of the data to a pair of vector and String. Vector is the first two numbers which represent
        // coordinates and string is the tweet text.
        JavaPairRDD<Vector, String> parsedData = data.mapToPair(s -> {
            //Each value is separated with ",".But because some sentences may have "," in the middle,
            // the regular expression should be "separate by comma where it is not followed by space".
            String[] splittedValues = s.split(",(?=\\S)");
            String tweetText = splittedValues[splittedValues.length - 1];
            double[] coordinates = new double[2];
            coordinates[0] = Double.parseDouble(splittedValues[0]);
            coordinates[1] = Double.parseDouble(splittedValues[1]);
            return new Tuple2<>(Vectors.dense(coordinates), tweetText);
        });

        //Cache the parsed data because k- means is a iterator algorithm and will iterate multiple times on data.
        // So, it is better to have data in memory to increase speed.
        parsedData.cache();
        // Cluster the data into two classes using KMeans
        int numClusters = 4;
        int numIterations = 100;
        //Create 4 clusters from whole data
        KMeansModel clusters = KMeans.train(parsedData.map(d -> d._1).rdd(), numClusters, numIterations);
        //To find which tweet is in which of the 4 clusters, predict function should be used.
        // So, the below line generates a pair os integer ans string in which integer is the number of cluster and
        // string is the tweet text.
        // Also, it sorts the result by number of cluster.
        JavaPairRDD<Integer, String> result = parsedData.mapToPair(d -> new Tuple2<>(clusters.predict(d._1), d._2))
                                                        .sortByKey();
        //Print the results
        result.foreach(r -> System.out.println("Tweet \"" + r._2 + "\" is in cluster " + r._1));

        sc.stop();
    }
}
