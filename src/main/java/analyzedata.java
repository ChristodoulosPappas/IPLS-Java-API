import io.ipfs.api.IPFS;
import jnr.ffi.annotations.In;
import org.javatuples.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.*;

public class analyzedata {


    public static Map<Integer,String> Find_Candidates(){
        List<String> Candidates = new ArrayList<>();

        List<Integer> Least_Replicated = new ArrayList<>();
        Least_Replicated.add(0);
        Least_Replicated.add(1);
        Least_Replicated.add(2);
        Least_Replicated.add(3);
        Least_Replicated.add(4);
        Least_Replicated.add(6);


        //Initialize priority queue, of the form (Peer,Auth_Size), and put all known peers
        PriorityQueue<org.javatuples.Pair<String,Integer>> queue = new PriorityQueue<>(new Comparator<org.javatuples.Pair<String, Integer>>() {
            @Override
            public int compare(org.javatuples.Pair<String, Integer> objects, Pair<String, Integer> t1) {
                return objects.getValue1()-t1.getValue1();
            }
        });
        PriorityQueue<org.javatuples.Pair<String,Integer>> helpQueue = queue;

        List<String> Peers = new ArrayList<>();
        Map<Integer,String> Candidates_Map = new HashMap<>();
        Pair<String,Integer> Peer = null;

        Peers.add("aaa");
        Peers.add("bbb");
        Peers.add("vvv");
        Peers.add("ass");

        for(int i = 0; i < Peers.size(); i++){
            queue.add(new Pair<>(Peers.get(i),i+1));
        }


        for (int i = 0; i < Least_Replicated.size(); i++) {
            for (int j = 0; j < queue.size(); j++) {
                Peer = helpQueue.remove();
                if (true) {
                    Candidates_Map.put(Least_Replicated.get(i), Peer.getValue0());
                    queue.remove(Peer);
                    queue.add(new Pair<>(Peer.getValue0(), Peer.getValue1() + 1));
                    break;
                }
            }
            helpQueue = queue;
        }

        for(int i = 0; i < Peers.size(); i++){
            System.out.println(queue.remove());
        }

        return Candidates_Map;
    }

    public static void main(String[] argc) throws Exception {

        PeerData.Auth_List.add(1);
        System.out.println(PeerData.Auth_List);
        Thread.sleep(10000);

    	/*
    	List<Integer> Auth = new ArrayList<>();
        List<String> Peers = new ArrayList<>();

        Auth.add(1);
        Auth.add(2);
        Auth.add(3);

        Peers.add("aaa");
        Peers.add("bbb");
        Peers.add("vvv");
        Peers.add("ass");
        Peers.add("xxx");

        MyIPFSClass ipfsClass = new MyIPFSClass("/ip4/127.0.0.1/tcp/5001");
        String decodedString = ipfsClass.Marshall_Packet(Auth,Peers);

        byte[] bytes_array = Base64.getUrlDecoder().decode(decodedString);
        int OriginPeerSize,PeerSize,pid;
        List<Integer> Peer_Auth = new ArrayList<Integer>();
        ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);
        String Renting_Peer = null,Origin_Peer= null;
        //Get Pid
        pid = rbuff.getShort();
        System.out.println(ipfsClass.Get_RMap(rbuff,bytes_array));
        
        */
        
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
        List<Double> data1 = new ArrayList<>();
        List<Double> data2 = new ArrayList<>();
        List<Double> data3 = new ArrayList<>();
        List<Double> data4 = new ArrayList<>();

        FileInputStream file = new FileInputStream("ChartData");
        ObjectInputStream in = new ObjectInputStream(file);

        // Method for deserialization of object
        data1 = (List<Double>) in.readObject();
        file = new FileInputStream("ChartData(1)");
        in = new ObjectInputStream(file);
        data2 = (List<Double>) in.readObject();

        file = new FileInputStream("ChartData(2)");
        in = new ObjectInputStream(file);
        data3 = (List<Double>) in.readObject();

        file = new FileInputStream("ChartData(3)");
        in = new ObjectInputStream(file);
        data4 = (List<Double>) in.readObject();

        for(int i = 0; i < data1.size(); i++){
            System.out.println(data1.get(i) );
        }
        System.out.println("=======================================");
        System.out.println("=======================================");
        System.out.println("=======================================");
        System.out.println("=======================================");

*/
        /*
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
