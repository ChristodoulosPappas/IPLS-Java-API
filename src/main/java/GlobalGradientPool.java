import io.ipfs.api.IPFS;
import io.ipfs.api.Sub;
import io.ipfs.multibase.Base58;
import org.javatuples.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.internal.ExactComparisonCriteria;
import org.web3j.abi.datatypes.Bool;

import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


// This thread gets gradients and aggregates them into its weights
class GGP_Receiver extends Thread{
    MyIPFSClass ipfsClass;
    IPFS ipfs;
    IPLS_Comm commit;
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_RESET = "\u001B[0m";

    // Increment the number of participants in order to average correctly
    // the replicas
    void update_participants(int partition,int num_of_participants){
        if(!PeerData.Participants.containsKey(partition)){
            PeerData.Participants.put(partition,num_of_participants);
        }
        else{
            PeerData.Participants.replace(partition,PeerData.Participants.get(partition) + num_of_participants);
        }
    }


    // this method scales the gradient vector to the number of participants so that the averaging
    // of all replicas could lead to a correct result. For example let R1 and R2 two replica vectors
    // aggregated from 4 and 2 different participants. Then the updated vector will be (4*R1 + 2*R2)/6
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

    public void process(String RecvData) throws Exception {
        if (PeerData.isBootsraper) {
            return;
        }
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
        if (pid == 3 && !PeerData.isBootsraper) {
            // tuple : Peer ID, iteration, number of peers, gradients
            Quartet<String, Integer, Integer, double[]> tuple = ipfsClass.Get_Replica_Model(rbuff, bytes_array);

            // check if message comes from other replica aggregator and also it arrives in time
            if (!tuple.getValue0().equals(PeerData._ID) && ipfsClass.synch_elapse_time(tuple.getValue1()) != -1 && ipfsClass.synch_elapse_time(tuple.getValue1()) > ipfsClass.get_curr_time()) {
                // newtuple : Replica_ID,partition,iteration,false,gradients
                Sextet<List<String>, Integer, Integer, Boolean, double[], String> newtuple = new Sextet<>(new ArrayList<>(Collections.singleton(tuple.getValue0())), partition, tuple.getValue1(), false, tuple.getValue3(), null);
                update_participants(partition, tuple.getValue2());
                // Because replica sent his aggregated partition, peer doesn't need the gradients he downloaded
                // so he can remove them
                PeerData.com_mtx.acquire();
                if (PeerData.Other_Replica_Gradients.containsKey(new Pair<>(partition, newtuple.getValue0()))) {
                    PeerData.Other_Replica_Gradients.remove(new Pair<>(partition, newtuple.getValue0()));
                    PeerData.Other_Replica_Gradients_Received.remove(new Pair<>(partition, newtuple.getValue0()));
                }
                PeerData.Received_Replicas.add(new Pair<>(partition, tuple.getValue0()));
                PeerData.com_mtx.release();

                PeerData.queue.add(newtuple);
            } else {
                tuple = null;
            }
        } else if (pid == 23 && !PeerData.isBootsraper ) {
            //Receive commit <Partition,Iteration,Hash, origin_peer>
            Quartet<Integer, Integer, String, String> Reply = ipfsClass.Get_Commitment(rbuff, bytes_array);
            // String : Hash, String : Aggregator, int : iteration , int partition
            if(!PeerData._ID.equals(Reply.getValue3()) && PeerData.Auth_List.contains(Reply.getValue0())){
                System.out.println(ANSI_RED + " " + Reply+ ANSI_RESET);
                PeerData.partial_updates_download_scheduler.add_partial_update(new Quartet(Reply.getValue2(),Reply.getValue3(),Reply.getValue1(),Reply.getValue0()));
            }
            //commit.process_commitment(Reply.getValue0(), Reply.getValue3(), Reply.getValue2(), Reply.getValue1(), Reply.getValue4());
        }
        /*
        else if(pid == 33 && !PeerData.isBootsraper){
            // Receive Partition,iteration Hash, origin_peer and secret key

            Quintet<Integer,Integer,String,String, SecretKey> Reply = ipfsClass.Get_SecretKey(rbuff,bytes_array);
            int Partition = Reply.getValue0();
            String Hash = Reply.getValue2();
            //In case that the file is already downloaded, then just decrypt it and add it to the
            // updater queue. Otherwise add the key to Hash_keys in order for the file to be decrypted
            // whenever it is downloaded
            PeerData.com_mtx.acquire();
            if(PeerData.Downloaded_Hashes.containsKey(Reply.getValue2())){
                PeerData.com_mtx.release();

                // If the hash is destined for me perform as usual
                if(PeerData.Downloaded_Hashes.get(Hash).equals(PeerData._ID)){
                    PeerData.queue.add(new Sextet<>(new ArrayList<>(Collections.singleton(Reply.getValue3())),Reply.getValue0(),Reply.getValue1(),true,(List<Double>) ipfsClass.decrypt(ipfsClass.Get_bytes(Reply.getValue2()),Reply.getValue4()),null));
                    PeerData.Downloaded_Hashes.remove(Reply.getValue2());
                }
                // In case the key is destined for other aggregator responsible for the same partition
                // aggregate it
                else{
                    String Aggregator = PeerData.Downloaded_Hashes.get(Hash);
                    if(PeerData.Other_Replica_Gradients.containsKey(new Pair<>(Partition,PeerData.Downloaded_Hashes.get(Hash)))){
                        List<Double> gradients = (List<Double>) ipfsClass.decrypt(ipfsClass.Get_bytes(Hash),PeerData.Hash_Keys.get(Hash));
                        for(int j = 0; j < gradients.size(); j++){
                            PeerData.Other_Replica_Gradients.get(new Pair<>(Partition,Aggregator)).set(j,PeerData.Other_Replica_Gradients.get(new Pair<>(Partition,Aggregator)).get(j) + gradients.get(j));
                        }
                        gradients = null;
                        PeerData.Other_Replica_Gradients_Received.put(new Pair<>(Partition,Aggregator),PeerData.Other_Replica_Gradients_Received.get(new Pair<>(Partition,Aggregator))+1);
                    }
                    else{
                        PeerData.Other_Replica_Gradients.put(new Pair<>(Partition,Aggregator),(List<Double>) ipfsClass.decrypt(ipfsClass.Get_bytes(Hash),PeerData.Hash_Keys.get(Hash)));
                        PeerData.Other_Replica_Gradients_Received.put(new Pair<>(Partition,Aggregator),1);
                    }
                }
            }
            else{
                PeerData.Hash_Keys.put(Reply.getValue2(),Reply.getValue4());
                PeerData.com_mtx.release();
            }

        }

         */
        else{

            PeerData.mtx.acquire();
            short is_reply = rbuff.getShort();
            org.javatuples.Triplet<String,Integer,Integer> ReplyPair = ipfsClass.Get_JoinRequest(rbuff,bytes_array);
            if(is_reply == 0 && PeerData.Auth_List.contains(partition) && !PeerData._ID.equals(ReplyPair.getValue0()) && !PeerData.New_Replicas.get(ReplyPair.getValue1()).contains(ReplyPair.getValue0()) && !PeerData.Replica_holders.get(ReplyPair.getValue1()).contains(ReplyPair.getValue0())){
                System.out.println("MSG REPLICA RECEIVED");
                while(ipfsClass.find_iter() == -1){
                    Thread.yield();
                }
                if(ipfsClass.training_elapse_time(ipfsClass.find_iter()) > ipfsClass.get_curr_time()){
                    if(!PeerData.Replica_holders.get(partition).contains(ReplyPair.getValue0())){
                        PeerData.Replica_holders.get(partition).add(ReplyPair.getValue0());
                        if(!PeerData.Replica_Wait_Ack.contains(new Triplet<>(ReplyPair.getValue0(),partition,ipfsClass.find_iter()))){
                            PeerData.Replica_Wait_Ack.add(new Triplet<>(ReplyPair.getValue0(),partition,ipfsClass.find_iter()));
                        }
                    }
                }
                else{
                    PeerData.New_Replicas.get(ReplyPair.getValue1()).add(ReplyPair.getValue0());
                }

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
        commit = new IPLS_Comm(PeerData.Path);
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
