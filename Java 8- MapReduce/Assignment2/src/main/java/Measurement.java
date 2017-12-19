public class Measurement {
    int time;
    double temperature;

    public Measurement(int time, double temperature) {
        this.time = time;
        this.temperature = temperature;
    }

    public Measurement(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "Measurement{" +
                "time=" + time +
                ", temperature=" + temperature +
                '}';
    }
}
