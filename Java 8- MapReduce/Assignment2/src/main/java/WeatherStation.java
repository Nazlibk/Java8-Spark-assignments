import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WeatherStation {

    static List<WeatherStation> stations = new ArrayList<>();
    String city;
    List<Measurement> measurements = new ArrayList<>();

    public static void main(String... args) {
        q1Test();
        q1Test();

        q2Test1();
        q2Test2();
    }

    /**This is a function for testing question1 (maxTemperature method) which is called in main
     */
    public static void q1Test() {
        WeatherStation weatherStation = new WeatherStation();
        //These are the variables used for generating 10 random measurements in which time is between 0 and 24 and
        // temperature is between -5 and 45.
        int maxForTime = 24;
        int minForTime = 0;
        double maxForTemperature = 45.0;
        double minForTemperature = -5.0;
        Random random = new Random();
        int time;
        double temperature;
        int n = 10;
        for (int i = 0; i < n; i++) {
            time = random.nextInt(maxForTime - minForTime + 1) + minForTime;
            temperature = minForTemperature + (maxForTemperature - minForTemperature) * random.nextDouble();
            weatherStation.measurements.add(new Measurement(time, temperature));
        }

        //prints the list of measurement created above
        System.out.println("Measurements values are: ");
        weatherStation.measurements.forEach(m -> System.out.println(m.toString()));
        System.out.println();

        //prints the maximum temperature between startTime and endTime
        System.out.println("Maximum temperature between hours 0 and 24 is: " + weatherStation.maxTemperature(0, 24));
        System.out.println("----------------------------------------------------------------");
    }

    public double maxTemperature(int startTime, int endTime) {
        return this.measurements.stream().filter(m -> (m.time >= startTime && m.time <= endTime)).
                map(m -> m.temperature).max(Comparator.comparing(i -> i)).get();
    }

    /**This is a function for testing question2 (maxTemperature method) which is called in main
     */
    public static void q2Test1() {
        //These are th variables for testing the case in assignment's PDF file
        double t1 = 19.0;
        double t2 = 10.8;
        double r = 2.1;
        List<Measurement> measurements1 = new ArrayList<>();
        measurements1.add(new Measurement(20.0));
        measurements1.add(new Measurement(11.7));
        measurements1.add(new Measurement(-5.4));
        measurements1.add(new Measurement(18.7));
        measurements1.add(new Measurement(20.9));
        List<Measurement> measurements2 = new ArrayList<>();
        measurements2.add(new Measurement(8.4));
        measurements2.add(new Measurement(19.2));
        measurements2.add(new Measurement(7.2));
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

        System.out.print("Result for countTemperature for t1 = " + t1 + ", t2 = " + t2 + " and r = " + r + " is: ");
        System.out.println(weatherStation.countTemperatures(t1, t2, r).toString());
        System.out.println("----------------------------------------------------------");
    }

    /**This is a function for testing question2 (maxTemperature method) which is called in main
     */
    public static void q2Test2() {
        //These are th variables for testing the case in assignment's PDF file
        double t1 = 20.4;
        double t2 = 5.8;
        double r = 7.1;
        List<Measurement> measurements1 = new ArrayList<>();
        measurements1.add(new Measurement(-4.9));
        measurements1.add(new Measurement(15.7));
        measurements1.add(new Measurement(-3.4));
        measurements1.add(new Measurement(16.7));
        measurements1.add(new Measurement(2.9));
        List<Measurement> measurements2 = new ArrayList<>();
        measurements2.add(new Measurement(5.4));
        measurements2.add(new Measurement(20.2));
        measurements2.add(new Measurement(-4.2));
        measurements2.add(new Measurement(14.5));
        measurements2.add(new Measurement(12.4));
        measurements2.add(new Measurement(23.7));
        measurements2.add(new Measurement(4.2));
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
        final AtomicInteger index = new AtomicInteger(0);
        stations.forEach(s -> {
            System.out.println("Measurements values for station " + index.addAndGet(1) + " are: ");
            s.measurements.forEach(m -> System.out.println(m.toString()));
            System.out.println();
        });

        System.out.print("Result for countTemperature for t1 = " + t1 + ", t2 = " + t2 + " and r = " + r + " is: ");
        System.out.println(weatherStation.countTemperatures(t1, t2, r).toString());
        System.out.println("----------------------------------------------------------");
    }

    public List<TemperatureNumberPair> countTemperatures(double t1, double t2, double r) {
        List<TemperatureNumberPair> temperatureNumberPairs = new ArrayList<>();
        for (WeatherStation ws : stations) {
            temperatureNumberPairs.addAll(map(ws, t1, t2, r));
        }
        return reduce(temperatureNumberPairs);
    }

    /**This is a map function. It gets a weatherStation, applies the filter.
     *
     * @param weatherStation
     * @param t1
     * @param t2
     * @param r
     * @return a list of TemperatureNumberPair in which the first element of the pair is t1 or t2 and second element is 1.
     * So, this function returns a list which length is equal to number of temperatures which are in the range of
     * [t1 - r .. t1 + r] or [t2 - r .. t2 + r].
     */

    public List<TemperatureNumberPair> map(WeatherStation weatherStation, double t1, double t2, double r) {
        List<TemperatureNumberPair> temperatureNumberPairs = new ArrayList<>();
        List<Measurement> count1;
        List<Measurement> count2;
        //This is the list of measurements in the range of [t1 - r .. t1 + r]
        count1 = weatherStation.measurements.parallelStream()
                                            .filter(m -> (m.temperature >= t1 - r && m.temperature <= t1 + r))
                                            .collect(Collectors
                                                    .toList());
        //This is the list of measurements in the range of [t2 - r .. t2 + r]
        count2 = weatherStation.measurements.parallelStream()
                                            .filter(m -> (m.temperature >= t2 - r && m.temperature <= t2 + r))
                                            .collect(Collectors.toList());
        //These two fors create a list of TemperatureNumberPair pairs by adding 1 as a second element (and t1 or t2 is the first element
        // of pair) of each above list and merging them to one list
        for (Measurement m : count1) {
            temperatureNumberPairs.add(new TemperatureNumberPair(t1, 1));
        }
        for (Measurement m : count2) {
            temperatureNumberPairs.add(new TemperatureNumberPair(t2, 1));
        }
        return temperatureNumberPairs;
    }

    /**This is a reduce function. It gets a list of TemperatureNumberPair in which the first element of the pair is t1 or t2
     * and the second element of the pair is 1. So, this method reduces this list to a map of <key, value> with length 2.
     * The first key is t1 and its value is the counts of numbers in the range of [t1 - r .. t1 + r]
     * and the second key is t2 and its value is the counts of numbers in the range of [t2 - r .. t2 + r].
     * Then, it converts the map to the list of TemperatureNumberPair pairs.
     *
     * @param temperatureNumberPairs: a list of TemperatureNumberPair in which the first element of the pair is t1 or t2
     * and the second element of the pair is 1.
     */
    public List<TemperatureNumberPair> reduce(List<TemperatureNumberPair> temperatureNumberPairs) {
        List<TemperatureNumberPair> tempNumPairs = new ArrayList<>();
        //groupingBy method in line below do the shuffle part and summingInt method do the reduce part
        Map<Double, Integer> collect = temperatureNumberPairs.stream()
                                                             .collect(Collectors.groupingBy(TemperatureNumberPair::getTemperature,
                                                                     Collectors.summingInt(TemperatureNumberPair::getCount)));
        //Convert map to the list of pairs
        for (double key : collect.keySet()) {
            tempNumPairs.add(new TemperatureNumberPair(key, collect.get(key)));
        }
        return tempNumPairs;
    }
}
