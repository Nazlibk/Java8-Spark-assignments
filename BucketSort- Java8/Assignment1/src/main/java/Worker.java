import java.util.ArrayList;
import java.util.Collections;

//This is a task class of threads
public class Worker implements Runnable {

    ArrayList<Integer> arrayList = new ArrayList<Integer>();

    public void run() {
        Collections.sort(arrayList);
    }

    public Worker(ArrayList<Integer> numberList) {
        this.arrayList = numberList;
    }
}
