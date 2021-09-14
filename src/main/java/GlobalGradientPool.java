import io.ipfs.api.IPFS;
import io.ipfs.api.Sub;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.internal.ExactComparisonCriteria;
import org.web3j.abi.datatypes.Bool;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


// This thread gets gradients and aggregates them into its weights
class GGP_Receiver extends Thread{
    MyIPFSClass ipfsClass;
    IPFS ipfs;

    void update_participants(int partition,int num_of_participants){
        if(!PeerData.Participants.containsKey(partition)){
            PeerData.Participants.put(partition,num_of_participants);
        }
        else{
            PeerData.Participants.replace(partition,PeerData.Participants.get(partition) + num_of_participants);
        }
    }

    public List<Double> get_replica_update(List<Double> replica, int participants){
        List<Double> updated_replica = new ArrayList<>();
        for(int i = 0; i < replica.size(); i++){
            updated_replica.add(replica.get(i)*participants);
        }
        return updated_replica;
    }

    public byte[] get_bytes(JSONObject obj){
        String decodedString,encodedString;
        byte[] decodedBytes;
        encodedString = (String) obj.get("data");
        decodedBytes = Base64.getUrlDecoder().decode(encodedString);
        decodedString = new String(decodedBytes);
        return  Base64.getUrlDecoder().decode(decodedString);
    }

    public int get_topic(JSONObject obj){
        return new Integer(((JSONArray) obj.get("topicIDs")).get(0).toString());
    }

    public void process(String RecvData) throws Exception{
        byte[] bytes_array;
        ByteBuffer rbuff;
        int partition;
        short pid;
        JSONObject obj;
        ipfsClass = new MyIPFSClass(PeerData.Path);

        obj = new JSONObject(RecvData);
        partition = get_topic(obj);
        bytes_array = get_bytes(obj);
        rbuff = ByteBuffer.wrap(bytes_array);
        pid = rbuff.getShort();
        if(pid != 8){
            Quartet<String,Integer,Integer,List<Double>> tuple = ipfsClass.Get_Replica_Model(rbuff,bytes_array);
            if(PeerData.isBootsraper) {
                tuple = null;
                return;
            }
            if(!tuple.getValue0().equals(PeerData._ID)){
                Quintet<String,Integer,Integer,Boolean,List<Double>> newtuple = new Quintet<>(tuple.getValue0(),partition,tuple.getValue1(),false,get_replica_update(tuple.getValue3(),tuple.getValue2()));
                update_participants(partition,tuple.getValue2());
                PeerData.queue.add(newtuple);
            }
            else{
                tuple = null;
            }
        }
        else{
            PeerData.mtx.acquire();
            short is_reply = rbuff.getShort();
            org.javatuples.Triplet<String,Integer,Integer> ReplyPair = ipfsClass.Get_JoinRequest(rbuff,bytes_array);
            if(is_reply == 0 && PeerData.Auth_List.contains(partition) && !PeerData._ID.equals(ReplyPair.getValue0()) && !PeerData.New_Replicas.get(ReplyPair.getValue1()).contains(ReplyPair.getValue0()) && !PeerData.Replica_holders.get(ReplyPair.getValue1()).contains(ReplyPair.getValue0())){
                PeerData.New_Replicas.get(ReplyPair.getValue1()).add(ReplyPair.getValue0());
                ipfs.pubsub.pub(ReplyPair.getValue0(),ipfsClass.JOIN_PARTITION_SERVER(PeerData._ID,partition,(short)3));
            }
            else if(is_reply == 1 && PeerData.Auth_List.contains(partition) && !PeerData._ID.equals(ReplyPair.getValue0())){
                PeerData.Replica_Wait_Ack.remove(new Triplet<>(ReplyPair.getValue0(),partition,PeerData.middleware_iteration));
                PeerData.Replica_holders.get(partition).remove(ReplyPair.getValue0());
                PeerData.New_Replicas.get(partition).remove(ReplyPair.getValue0());
            }

            PeerData.mtx.release();

        }


    }

    public void run()  {
        String RecvData;
        ipfs = new IPFS(PeerData.Path);
        while(true){
            try {
                RecvData = PeerData.GGP_queue.take();
                process(RecvData);

            }
            catch (Exception e) {
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
