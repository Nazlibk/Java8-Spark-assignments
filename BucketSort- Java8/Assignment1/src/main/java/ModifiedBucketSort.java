import java.util.ArrayList;
import java.util.Collections;

//This code is the solution of Q2
public class ModifiedBucketSort {

    public static int[] bucketSort(int[] numbers, int bucketCount, helperSort helperSortFunction) {

        if (numbers.length <= 1) return numbers;
        int maxVal = numbers[0];
        int minVal = numbers[0];

        for (int i = 1; i < numbers.length; i++) {
            if (numbers[i] > maxVal) maxVal = numbers[i];
            if (numbers[i] < minVal) minVal = numbers[i];
        }

        double interval = ((double)(maxVal - minVal + 1)) / bucketCount; // range of bucket
        ArrayList<Integer> buckets[] = new ArrayList[bucketCount];

        for (int i = 0; i < bucketCount; i++) // initialize buckets (initially empty)
            buckets[i] = new ArrayList<Integer>();

        for (int i = 0; i < numbers.length; i++) // distribute numbers to buckets
            buckets[(int)((numbers[i] - minVal)/interval)].add(numbers[i]);

        int k = 0;

        for (int i = 0; i < buckets.length; i++) {


            helperSortFunction.sortArray(buckets[i]); // calls Java's built-in merge sort (as a kind of “helper” sort)

            for (int j = 0; j < buckets[i].size(); j++) { // update array with the bucket content
                numbers[k] = buckets[i].get(j);
                k++;
            }
        }

        return numbers;
    }

    //An abstract helperSort class
    public abstract static class helperSort {
        public abstract void sortArray(ArrayList<Integer> arrayList);
    }

    public static void main(String... args){
        int[] numbersList = {4, 6, 3, 2, 8, 1, 9, 4, 0, 12, 45, 23};//Main list

        //Print main array
        System.out.println("Array before sort:");
        for(int element: numbersList){
            System.out.print(element + " ");
        }
        System.out.println();

        int[] sortedList;//Sorted array list
        int bucketCount = 4;

        //Anonymous sort class (helperSort). This anonymous class uses Collections.sort but you can use any sort algorithm.
        //You just need to change the body of the sortArray method below.
        sortedList = bucketSort(numbersList, bucketCount, new helperSort() {
            public void sortArray(ArrayList<Integer> arrayList) {
                Collections.sort(arrayList);
            }
        });//Invoking bucketSort method with anonymous class as its parameter

        //Print sorted array
        System.out.println("Array after sort:");
        for(int element: sortedList){
            System.out.print(element + " ");
        }
    }
}
