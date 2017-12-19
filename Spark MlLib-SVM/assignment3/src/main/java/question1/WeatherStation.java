package question1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class WeatherStation implements Serializable {

    static List<WeatherStation> stations = new ArrayList<>();
    static JavaSparkContext sc;
    String city;
    List<Measurement> measurements = new ArrayList<>();

    public static void main(String... args) throws InterruptedException {
        //JavaSparkContext configuration
        sc = new JavaSparkContext(new SparkConf().setAppName("Assignment3").setMaster("local"));

        int t = 11;
        List<Measurement> measurements1 = new ArrayList<>();
        measurements1.add(new Measurement(20.0));
        measurements1.add(new Measurement(11.7));
        measurements1.add(new Measurement(10.7));
        measurements1.add(new Measurement(-5.4));
        measurements1.add(new Measurement(18.7));
        measurements1.add(new Measurement(20.9));
        measurements1.add(new Measurement(11.5));
        measurements1.add(new Measurement(11));
        measurements1.add(new Measurement(10));
        measurements1.add(new Measurement(12));
        List<Measurement> measurements2 = new ArrayList<>();
        measurements2.add(new Measurement(8.4));
        measurements2.add(new Measurement(19.2));
        measurements2.add(new Measurement(7.2));
        measurements2.add(new Measurement(11.7));
        measurements2.add(new Measurement(11.7));
        measurements2.add(new Measurement(11.7));
        WeatherStation weatherStation1 = new WeatherStation();
        weatherStation1.measurements.addAll(measurements1);
        WeatherStation weatherStation2 = new WeatherStation();
        weatherStation2.measurements.addAll(measurements2);
        //There are two weatherStations which should be added to stations
        stations.add(weatherStation1);
        stations.add(weatherStation2);
        WeatherStation weatherStation = new WeatherStation();
        System.out.println();

        //prints the list of measurements in station
        AtomicInteger index = new AtomicInteger(0);
        stations.forEach(s -> {
            System.out.println("Measurements values for station " + index.addAndGet(1) + " are: ");
            s.measurements.forEach(m -> System.out.println(m.toString()));
            System.out.println();
        });

        System.out.print("Result for countTemperature between " + (t - 1) + " and " + (t + 1) + " is: ");
        System.out.println(weatherStation.countTemperature(t));
        System.out.println("----------------------------------------------------------");
    }


    public static int countTemperature(int t) {
        //This line of code parallelize WeatherStations. So, whole the statements below this line will be in parallel to
        // each other for each WeatherStation.
        JavaRDD<WeatherStation> stationJavaRDD = sc.parallelize(stations);
        return stationJavaRDD.map(ws -> ws.measurements.stream().
                filter(n -> (n.temperature >= t - 1 && n.temperature <= t + 1)).count())
                             .reduce((a, b) -> a + b)
                             .intValue();
    }
}
