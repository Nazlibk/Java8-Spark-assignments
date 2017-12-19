public class TemperatureNumberPair {
    private double temperature;
    private int count;

    public TemperatureNumberPair(double temperature, int count) {
        this.temperature = temperature;
        this.count = count;
    }

    public double getTemperature() {
        return temperature;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "(" + temperature + ", " + count + ")";
    }
}
