import io.ipfs.api.IPFS;
import org.javatuples.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class analyzedata {
    public static void main(String[] argc) throws Exception {

        PriorityQueue<Pair<String,Integer>> queue = new PriorityQueue<>(new Comparator<Pair<String, Integer>>() {
            @Override
            public int compare(Pair<String, Integer> objects, Pair<String, Integer> t1) {
                return objects.getValue1()-t1.getValue1();
            }
        });

        for(int i = 0; i < 9; i++){
            queue.add(new Pair<>("al" + i, i+1));
        }

        for(int i = 0; i < 9; i++){
            System.out.println(queue.remove());
        }

        /*
        List<Double> data1 = new ArrayList<>();


        FileInputStream file = new FileInputStream("ChartData");
        ObjectInputStream in = new ObjectInputStream(file);

        // Method for deserialization of object
        data1 = (List<Double>) in.readObject();
        for(int i = 0; i < data1.size(); i++){
            System.out.println(data1.get(i));
        }
        */
        /*

        List<Double> data2 = new ArrayList<>();
        List<Double> data3 = new ArrayList<>();
        List<Double> data4 = new ArrayList<>();

        FileInputStream file = new FileInputStream("Uniform/Node1");
        ObjectInputStream in = new ObjectInputStream(file);

        // Method for deserialization of object
        data1 = (List<Double>) in.readObject();
        file = new FileInputStream("Uniform/Node2");
        in = new ObjectInputStream(file);
        data2 = (List<Double>) in.readObject();

        file = new FileInputStream("Uniform/Node3");
        in = new ObjectInputStream(file);
        data3 = (List<Double>) in.readObject();

        file = new FileInputStream("Uniform/Node4");
        in = new ObjectInputStream(file);
        data4 = (List<Double>) in.readObject();

        for(int i = 0; i < data1.size(); i++){
            System.out.println((data1.get(i) + data2.get(i) +data3.get(i) +data4.get(i))/4);
        }
        System.out.println("=======================================");
        System.out.println("=======================================");
        System.out.println("=======================================");
        System.out.println("=======================================");

        file = new FileInputStream("non-uniform/01");
        in = new ObjectInputStream(file);

        // Method for deserialization of object
        data1 = (List<Double>) in.readObject();
        System.out.println(data1.size());
        file = new FileInputStream("non-uniform/234");
        in = new ObjectInputStream(file);
        data2 = (List<Double>) in.readObject();

        file = new FileInputStream("non-uniform/567");
        in = new ObjectInputStream(file);
        data3 = (List<Double>) in.readObject();

        file = new FileInputStream("non-uniform/89");
        in = new ObjectInputStream(file);
        data4 = (List<Double>) in.readObject();

        for(int i = 0; i < data1.size(); i++){
            System.out.println((data1.get(i) + data2.get(i) +data3.get(i) +data4.get(i))/4);
        }


         */

    }
}
