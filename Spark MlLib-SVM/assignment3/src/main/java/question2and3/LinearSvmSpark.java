package question2and3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.util.Arrays;


public class LinearSvmSpark {

    static JavaSparkContext sc;

    public static void main(String... args) {
        //JavaSparkContext configuration
        sc = new JavaSparkContext(new SparkConf().setAppName("Assignment3").setMaster("local"));

        //Read data from yelp_labelled.txt file to an JavaRDD.
        JavaRDD<String> lines = sc.textFile("yelp_labelled.txt");
        //Map each line of data to a pair of String and Integer in which String is the sentence(s) and Integer is its label.
        // While it generates pair of <String, Integer>, it also do some preprocessing on String. First converts all letters
        // of String to lower case. Second, it removes every character except a-z, 0-9, space and tab.
        // At the end, it removes some words such as a, the, an.
        JavaPairRDD<String, Integer> pairOfLinesAndLabels = lines.mapToPair(l -> new Tuple2<>(l.substring(0, l.length() - 2)
                                                                                               .toLowerCase()
                                                                                               .replaceAll("[^a-z0-9 \t]+", "")
                                                                                               .replaceAll("\\s*\\bthe\\b\\s*", "")
                                                                                               .replaceAll("\\s*\\ba\\b\\s*", "")
                                                                                               .replaceAll("\\s*\\ban\\b\\s*", "")
                , Integer.valueOf(l.toString().split("\t")[1])));

        //First, words frequency vectors extracted and then data is sampled. So, we have words frequency vectors for both training and test data.
        final HashingTF hashingTF = new HashingTF();
        //Create words frequency for each line of input file.
        JavaRDD<LabeledPoint> frequencyVectors = pairOfLinesAndLabels.map(l -> new LabeledPoint(l._2, hashingTF
                .transform(Arrays.asList(l._1.split(" ")))));

        //For getting more accurate results, I run the SVM on the same dataset multiple times and then averaged the results of all.
        int iteration = 5;
        double[] results = new double[iteration];
        for (int i = 0; i < iteration; i++) {

            //Split initial data to two separate part. 60% for training set and 40% for test set.
            JavaRDD<LabeledPoint> training = frequencyVectors.sample(false, 0.6);
            //Cache the frequencyVectors because SVM is a iterator algorithm.
            training.cache();
            //Put the other 40% to a variable named test which will be used for testing.
            JavaRDD<LabeledPoint> test = frequencyVectors.subtract(training);


            //Number of iterations
            int numIterations = 100;
            // Run training algorithm to build the model.
            SVMModel svmModel = SVMWithSGD.train(training.rdd(), numIterations);


            // Clear the default threshold so the model can return probability.
            svmModel.clearThreshold();

            // Compute raw scores on the test set.
            JavaRDD<Tuple2<Object, Object>> testSetLabels = test.map(p ->
                    new Tuple2<>(svmModel.predict(p.features()), p.label()));

            //print first 10 labels of prediction for test set
            testSetLabels.take(10).forEach(t -> System.out.println(t._2));

            //Question 3: Below codes are for getting result for AUROC. As mentioned before, SVM run on the dataset multiple
            // times and results are saved in variable results. Then the average is provided as final result.
            // Get evaluation metrics and put it in array
            results[i] = new BinaryClassificationMetrics(testSetLabels.rdd()).areaUnderROC();
        }
        //Calculate the average of multiple runs of SVM on the dataset
        double average = Arrays.stream(results).average().getAsDouble();
        System.out.println("Area under ROC (AUROC) = " + average);

        sc.stop();

    }
}
