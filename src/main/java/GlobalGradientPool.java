import io.ipfs.api.Sub;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


// This thread gets gradients and aggregates them into its weights
class GGP_Receiver extends Thread{
    MyIPFSClass ipfsClass;


    public void run()  {
        String RecvData,decodedString,encodedString;
        byte[] bytes_array,decodedBytes;
        ByteBuffer rbuff;
        int partition;
        JSONObject obj;
        JSONArray jArray;
        ipfsClass = new MyIPFSClass(PeerData.Path);

        while(true){
            try {
                RecvData = PeerData.GGP_queue.take();
                

                obj = new JSONObject(RecvData);

                encodedString = (String) obj.get("data");
                decodedBytes = Base64.getUrlDecoder().decode(encodedString);
                decodedString = new String(decodedBytes);
                jArray = (JSONArray) obj.get("topicIDs");
                partition = new Integer(jArray.get(0).toString());
                //Aggregate
                bytes_array = Base64.getUrlDecoder().decode(decodedString);
                rbuff = ByteBuffer.wrap(bytes_array);
                rbuff.getShort();
                Triplet<String,Integer, List<Double>> tuple = ipfsClass.Get_Gradients(rbuff,bytes_array);
                if(PeerData.isBootsraper) {
                	tuple = null; 
                	continue;
                }
                if(!tuple.getValue0().equals(PeerData._ID)){
                    tuple.setAt0(null);
                    Triplet<String,Integer,List<Double>> newtuple = new Triplet<>(null,tuple.getValue1(),tuple.getValue2());
                    PeerData.queue.add(newtuple);
                }
                else{
                    tuple = null;
                }

            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}



// In this thread we take care on what gradients the peer should subscribe
// based on his partition list. If any change happens in AuthList, the thread
// is going to be informed and destroy or create threads.

public class GlobalGradientPool extends Thread{
    BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    MyIPFSClass ipfsClass;

    public Map<Integer,Sub> init(){
        Sub thread;
        Map<Integer,Sub> Threads_Map = new HashMap<>();
        for(int i = 0; i < PeerData.Auth_List.size(); i++) {
            thread = new Sub(PeerData.Auth_List.get(i).toString(),PeerData.Path,PeerData.GGP_queue,true);
            thread.start();
            Threads_Map.put(PeerData.Auth_List.get(i),thread);
        }
        return Threads_Map;
    }


    public void run(){
        Map<Integer,Sub> Threads_Map = new HashMap<>();
        Pair<Integer, Integer> Task;
        Sub Thread;

        GGP_Receiver Recv = new GGP_Receiver();
        Recv.start();
        //Initialize thread map with the first Partition List
        Threads_Map = init();

        //Get partition changes in the form of a pair of integers (Action, Partition)
        // Where Action = 0, delete thread and  1 create
        while(true){
            try {
                Task = PeerData.UpdateQueue.take();
                if(Task.getValue0() == 1){
                    Thread = new Sub(Task.getValue1().toString(),PeerData.Path,PeerData.GGP_queue,true);
                    Thread.start();
                    Threads_Map.put(Task.getValue1(),Thread);
                }
                else{
                	System.out.println("Removing : " + Task);
                    //Check if partition exists in Threads Map
                    if(Threads_Map.containsKey(Task.getValue1())){
                        Threads_Map.get(Task.getValue1()).terminate();
                        Threads_Map.remove(Task.getValue0());
                    }

                }
            }
            catch (Exception e){

            }
        }


    }

}
