package question1;

import java.io.Serializable;

public class Measurement implements Serializable {
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
        return "question1.Measurement{" +
                "time=" + time +
                ", temperature=" + temperature +
                '}';
    }
}
