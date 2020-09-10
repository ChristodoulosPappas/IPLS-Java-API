import io.ipfs.api.*;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.apache.commons.math3.analysis.function.Add;
import org.javatuples.Triplet;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import org.json.JSONObject;

class ThreadReceiver extends Thread{
    int _PARTITIONS,_MIN_PARTITIONS;
    String Topic,_ID,Path;
    IPFS ipfs;
    MyIPFSClass AuxilaryIpfs = new MyIPFSClass(PeerData.Path);

    public ThreadReceiver(String path){
        Path = path;
        ipfs = new IPFS(path);
        Topic = "New_Peer";
    }

    public ThreadReceiver(String topic,String  path,int PARTITIONS,int MIN_PARTITIONS){
        Path = path;
        ipfs = new IPFS(path);
        Topic = topic;
        _PARTITIONS = PARTITIONS;
        _MIN_PARTITIONS = MIN_PARTITIONS;
    }

    //This function checks if peer belongs into peer's swarm
    public boolean _In_swarm(String PeerId) throws IOException {
        int i;
        try {
            List<Peer> Peers = ipfs.swarm.peers();
            for(i = 0; i < Peers.size(); i++){
                if(PeerId.equals(Peers.get(i).id.toString())){
                    return true;
                }
            }
            return false;
        }
        catch (Exception e){
            return false;
        }

    }
    //Partition Dealer that a peer selects, selects randomnly a series of
    //partitions to send. If peer is overloaded,then it sends all the
    public int[] Select_Partitions(){
        int rand;
        int i = 0;
        Random Randint = new Random();
        if(PeerData.Auth_List.size() > _MIN_PARTITIONS){
            int[] Auth = new int[_MIN_PARTITIONS];
            rand = Randint.nextInt(PeerData.Auth_List.size());
            for(i = 0; i < PeerData.Auth_List.size(); i++){
                Auth[i] = PeerData.Auth_List.get((i + rand)%PeerData.Auth_List.size());
            }
            return  Auth;
        }
        else{
            int[] Auth = new int[(int)_MIN_PARTITIONS/2];
            rand = Randint.nextInt(PeerData.Auth_List.size());
            for(i = 0; i < (int)(PeerData.Auth_List.size()/2); i++){
                Auth[i] = PeerData.Auth_List.get((i + rand)%PeerData.Auth_List.size());
            }
            return Auth;

        }

    }


    public Multihash _Upload_File(List<Double> Weights, MyIPFSClass ipfsClass, String filename) throws IOException {
        //Serialize the Partition model into a file
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(Weights);
        oos.close();
        fos.close();
        // Add file into ipfs system
        return  ipfsClass.add_file(filename);
    }

    //This function is called when an overloaded peer is able to remove
    // partitions that another peer wanted to have.
    public void Remove_Partitions(List<Integer> Peer_Auth){
        int i,remove_limit,counter = 0;
        List<Integer> Delete_Partitions = new ArrayList<Integer>();
        if(PeerData.Auth_List.size() - Peer_Auth.size() >= _MIN_PARTITIONS){
            remove_limit = Peer_Auth.size();
            for(i = 0; i < remove_limit; i++){
                if(PeerData.Auth_List.contains(Peer_Auth.get(i))){
                    PeerData.Auth_List.remove(Integer.valueOf(Peer_Auth.get(i)));
                    Delete_Partitions.add(Peer_Auth.get(i));
                    PeerData.UpdateQueue.add(new org.javatuples.Pair<>(0,Peer_Auth.get(i)));
                }
            }
        }
        else{
            remove_limit = PeerData.Auth_List.size() - _MIN_PARTITIONS;
            if(remove_limit == 0){
                return;
            }
            for(i = 0; i < Peer_Auth.size(); i++){
                if(PeerData.Auth_List.contains(Peer_Auth.get(i))){
                    PeerData.Auth_List.remove(Integer.valueOf(Peer_Auth.get(i)));
                    PeerData.UpdateQueue.add(new org.javatuples.Pair<>(0,Peer_Auth.get(i)));

                    Delete_Partitions.add(Peer_Auth.get(i));
                    counter++;
                    if(counter == remove_limit){
                        break;
                    }
                }
            }
        }
    }

    //THIS FUNCTION SHOULD BE EXECUTED IN ATOMIC MANNER
    public  void update_peer_structures(short pid,String Peer_Id, List<Integer> Peer_Auth,String renting_peer) {
        int i;
        String packet = null;
        List<String> Peers_id = new ArrayList<String>();
        List<Peer> Peers = null;
        try {
            Peers = ipfs.swarm.peers();
        }
        catch(Exception e){
            System.out.println("Unable to find peers");
        }
        List<Integer> Old_Peer_Auth = new ArrayList<Integer>();
        //In case remote peer adds in its authority list
        // Partitions we should add those in our data structures
        if(pid == 1 || pid == 2) {
            //Update existing peers list in case some peers left
            //Create String List with peer hash IDs
            for (i = 0; i < Peers.size(); i++) {
                Peers_id.add(Peers.get(i).id.toString());
            }

            //If the peer that is in my swarm and it is not a heartbeat message continue
            if (Peers_id.contains(Peer_Id) && (PeerData.Swarm_Peer_Auth.get(Peer_Id) == null || PeerData.Swarm_Peer_Auth.get(Peer_Id).equals(Peer_Auth) == false)) {
                //First remove old partitions of the peer
                if(PeerData.Swarm_Peer_Auth.get(Peer_Id) != null ){
                    for (i = 0; i < PeerData.Swarm_Peer_Auth.get(Peer_Id).size(); i++) {
                        PeerData.Partition_Availability.get(PeerData.Swarm_Peer_Auth.get(Peer_Id).get(i)).remove(Peer_Id);
                    }
                }

                PeerData.Swarm_Peer_Auth.put(Peer_Id, Peer_Auth);
                //Add in new Peer partitions its Peer id
                for (i = 0; i < Peer_Auth.size(); i++) {
                    PeerData.Partition_Availability.get(Peer_Auth.get(i)).add(Peer_Id);
                }
                //In case remote peer believes that peer is overloaded
                // peer must check if he actually is and then remove some
                // number of partitions and then notifies peers about its
                // new state
                if (renting_peer != null && renting_peer.equals(_ID)) {
                    System.out.println("Removing Partitions");
                    if (PeerData.Auth_List.size() > _MIN_PARTITIONS) {
                        Remove_Partitions(Peer_Auth);
                        try{
                            packet = AuxilaryIpfs.Marshall_Packet(PeerData.Auth_List, null, ipfs.id().get("ID").toString() ,(short) 2);
                        }
                        catch (Exception e){

                        }
                        if (packet != null) {
                            try{
                                ipfs.pubsub.pub("Authorities", packet);
                            }
                            catch (Exception e){
                                System.out.println("Unable to Pub");
                            }
                        }
                    }
                }
            }
        }
    }




    public void connect(String Addr)throws IOException{
        try {
        	ipfs.swarm.connect(new MultiAddress(Addr.replace("p2p","ipfs")));
        	System.out.println("Connected succesfully to a peer : " + Addr);
        }
        catch (Exception e){
        	System.out.println("Unable to connect to peer : " + Addr);
        }
    }

    public void process(String decodedString) throws Exception {
        int pid,is_reply,arr_len,i;
        String reply = null;
        String PeerId = null;

        //_ID Protocol Format
        // Protocol : GET_AUTH
        // [Pid = 1,ASK(0)];
        // [Pid = 1,Reply(1),num_of_auth,AUTH1,AUTH2,...,AUTH_NoA]
        byte[] bytes_array = Base64.getUrlDecoder().decode(decodedString);
        int OriginPeerSize,PeerSize;
        List<Integer> Peer_Auth = new ArrayList<Integer>();
        ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);
        String Renting_Peer = null,Origin_Peer= null;
        //Get Pid
        pid = rbuff.getShort();

        if(Topic == "Authorities" || Topic.equals(_ID)){
            //System.out.println("Protocol ID : " + pid);
            if(pid == 1) {
                arr_len = rbuff.getInt();
                for (i = 0; i < arr_len; i++) {
                    Peer_Auth.add(rbuff.getInt());
                }
                PeerSize = rbuff.getShort();
                OriginPeerSize = rbuff.getShort();
                byte[] Id_array = new byte[bytes_array.length - (arr_len + 1) * Integer.BYTES - 3*Short.BYTES -OriginPeerSize];
                byte[] Origin_array = new byte[bytes_array.length - (arr_len + 1) * Integer.BYTES - 3*Short.BYTES -PeerSize];


                for (i = (arr_len + 1) * Integer.BYTES + 3*Short.BYTES; i < (arr_len+1)*Integer.BYTES + 3*Short.BYTES + PeerSize; i++) {
                    Id_array[i - (arr_len + 1) * Integer.BYTES - 3*Short.BYTES] = bytes_array[i];
                }
                for (i = (arr_len+1)*Integer.BYTES + 3*Short.BYTES + PeerSize; i < bytes_array.length; i++){
                    Origin_array[i -((arr_len+1)*Integer.BYTES + 3*Short.BYTES + PeerSize)] = bytes_array[i];
                }
                Renting_Peer = new String(Id_array);
                Renting_Peer = Renting_Peer.trim();
                Origin_Peer = new String(Origin_array);
                Origin_Peer = Origin_Peer.trim();
                PeerId = Origin_Peer;

                if(PeerId.equals(ipfs.id().get("ID").toString())){
                    return;
                }
                //UPDATE DATA STRUCTURES
                update_peer_structures((short) pid,PeerId,Peer_Auth,Renting_Peer);

            }
            else if(pid == 2){
                Map<String,List<Integer>> pair = AuxilaryIpfs.Get_Partitions(rbuff,bytes_array);
                PeerId = pair.keySet().iterator().next();
                Peer_Auth = pair.get(PeerId);
                Renting_Peer = null;
                if(PeerId.equals(ipfs.id().get("ID").toString())){
                    return;
                }
                //UPDATE DATA STRUCTURES
                update_peer_structures((short) pid,PeerId,Peer_Auth,Renting_Peer);
            }
            // Get Update message
            else if(Topic.equals(_ID) && pid == 3){
                //String: Origin Peer , Integer : Partition, List<Double> : Gradients
                System.out.println("Getting Gradients");
                Triplet<String,Integer,List<Double>> tuple = AuxilaryIpfs.Get_Gradients(rbuff,bytes_array);
                //Put the request to the Updater

                PeerData.queue.add(tuple);
            }
            // Get ACK message
            else if(Topic.equals(_ID) && pid == 4){
                System.out.println("Updated Weights");
                //Triplet in the form < Partition, File Hash , Peer ID >
                Triplet<String,Integer,List<Double>> ReplyPair = AuxilaryIpfs.Get_Gradients(rbuff,bytes_array);
                org.javatuples.Pair<String,Integer> pair = new org.javatuples.Pair<>(ReplyPair.getValue0(),ReplyPair.getValue1());
                if(PeerData.Wait_Ack.contains(pair)){
                    PeerData.Wait_Ack.remove(pair);
                }
                PeerData.Weight_Address.put(ReplyPair.getValue1(),ReplyPair.getValue2());
            }
            else if(Topic.equals(_ID) && pid == 5){
                int j;
                List<Double> Weights = new ArrayList<>();

                for(i = 0; i < PeerData._PARTITIONS; i++){
                    for(j = 0; j < PeerData.Weights.get(i).size(); j++){
                        Weights.add(PeerData.Weights.get(i).get(j));
                    }
                }
                PeerId = AuxilaryIpfs.Get_Peer(rbuff,bytes_array,Short.BYTES);
                ipfs.pubsub.pub(PeerId,AuxilaryIpfs.Marshall_Packet(_Upload_File(Weights,AuxilaryIpfs,_ID + "Model").toString(),(short) 6));
            }
            else if(Topic.equals(_ID) && pid == 6){
                int j,chunksize = (int)(PeerData._MODEL_SIZE/PeerData._PARTITIONS) + 1;


                PeerData._Iter_Clock = rbuff.getInt();
                String fileHash;
                List<Double> Parameters = new ArrayList<Double>();
                fileHash = AuxilaryIpfs.Get_Peer(rbuff,bytes_array,Short.BYTES + Integer.BYTES);
                //System.out.println(fileHash.length());
                //System.out.println("FILE HASH " + fileHash);
                Parameters = AuxilaryIpfs.DownloadParameters(fileHash);
                //System.out.println("Model Parameters : " + Parameters.size());
                //System.out.println("CLOCK : " + PeerData._Iter_Clock);
                if(Parameters.size() == PeerData._MODEL_SIZE){
                    for(i = 0; i < PeerData._PARTITIONS; i++){
                        for(j = i*chunksize; j < (i+1)*chunksize && j < Parameters.size(); j++){
                            PeerData.Weights.get(i).set(i,Parameters.get(j));
                        }
                    }
                }
                System.out.println("MODEL LOADED");
                //System.out.println(PeerData.Weights.get(0).size());
                PeerData.Wait_Ack.remove(0);

            }
            //This PID is called mainly on Bootstrapers, and is used
            // in order to inform the new peer about other peers
            else if(Topic.equals(_ID) && pid == 7){
                is_reply = rbuff.getShort();
                if(is_reply == 0){
                    List<String> Client = AuxilaryIpfs.Get_MultiaddrPeers(rbuff,bytes_array);
                    //PeerId = Get_Peer(rbuff,bytes_array,Short.BYTES);
                    List<Peer> Peer_Multiaddr = ipfs.swarm.peers();
                    List<String> Peers = new ArrayList<>();
                    for(i = 0; i < Peer_Multiaddr.size(); i++){
                        Peers.add(Peer_Multiaddr.get(i).address + "/" + "p2p/" + Peer_Multiaddr.get(i).id);
                    }
                    Peers.add(_ID);
                    ipfs.pubsub.pub(Client.get(0),AuxilaryIpfs.Marshall_Packet(Peers,true));

                }
                else if(is_reply == 1){
                    List<String> Multiaddr = AuxilaryIpfs.Get_MultiaddrPeers(rbuff,bytes_array);
                    System.out.println("Multiaddr" + Multiaddr);
                    for(i = 0; i < Multiaddr.size()-1; i++){
                        String[] fractions = Multiaddr.get(i).split("/");
                        if(fractions[fractions.length-1].equals(_ID)){
                            System.out.println("MY PUBLIC MULTI-ADDRESS : " +  Multiaddr.get(i));
                            PeerData.MyPublic_Multiaddr = Multiaddr.get(i);
                        }
                        else{
                            //try to connect to peer
                            connect(Multiaddr.get(i));
                        }
                    }
                    org.javatuples.Pair<String,Integer>  tuple = new org.javatuples.Pair(Multiaddr.get(Multiaddr.size()-1),0);
                    PeerData.Wait_Ack.remove(tuple);
                }


            }
        }
        else if(Topic.equals("New_Peer")){
            rbuff.getShort();
            //Get peer data
            List<String> Peers = AuxilaryIpfs.Get_MultiaddrPeers(rbuff,bytes_array);
            PeerId = Peers.get(0);
            System.out.println("New peer msg from : " + PeerId);
            connect(Peers.get(1));
            if(_In_swarm(PeerId) && PeerId.equals(ipfs.id().get("ID").toString()) == false){
                //try to put peer in your swarm if you can
                reply = AuxilaryIpfs.Marshall_Packet(PeerData.Auth_List,null, ipfs.id().get("ID").toString(),(short) 2);
                if(reply != null) {
                    ipfs.pubsub.pub(PeerId,reply);
                }
                else {
                	System.out.println("Nulled msg");
                }
            }

        }

    }

    public void run() {
        byte[] decodedBytes;
        String encoded = null;
        String decodedString = null;
        Stream<Map<String, Object>> sub = null;
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>();


        Sub SUB = new Sub(Topic,Path,queue,true);
        SUB.start();
        String s = null;
        try {
            _ID = ipfs.id().get("ID").toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        PeerData.InitSem.release();
        while(true){

            try {
                s =  queue.take();
                JSONObject obj = new JSONObject(s);

                encoded = (String) obj.get("data");
                decodedBytes = Base64.getUrlDecoder().decode(encoded);
                decodedString = new String(decodedBytes);
                process(decodedString);

            }
            catch (Exception e) {
                e.printStackTrace();
            }


        }
    }
}


public class IPLS {
    String my_id;
    static IPFS ipfs;
    static MyIPFSClass ipfsClass;
    static int _PARTITIONS = PeerData._PARTITIONS;
    static int _MIN_PARTITIONS = PeerData._MIN_PARTITIONS;
    public static int num = 0;
    SwarmManager SwarmManagerThread;
    ThreadReceiver _New_Peer;
    ThreadReceiver _Personal_Thread;
    ThreadReceiver _Auth_Listener;
    Updater UpdaterThread;


    static void test(String decodedString){
        int pid,is_reply,arr_len,i;
        String reply = null;


        //_ID Protocol Format
        // Protocol : GET_AUTH
        // [Pid = 1,ASK(0)];
        // [Pid = 1,Reply(1),num_of_auth,AUTH1,AUTH2,...,AUTH_NoA]

            byte[] bytes_array = Base64.getUrlDecoder().decode(decodedString);
            ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);
            //Get Pid
            pid = rbuff.getShort();
            //Protocol : INFORM_PEER
            //[Pid = 2,num_of_auth,AUTH1,AUTH2...];
            if(pid == 2){
                List<Integer> Peer_Auth = new ArrayList<Integer>();
                arr_len = rbuff.getInt();

                for(i = 0; i < arr_len; i++){
                    Peer_Auth.add(rbuff.getInt());
                }
                for(i = 0; i < arr_len; i++){
                    System.out.println(Peer_Auth.get(i));
                }
                //UPDATE DATA STRUCTURES

            }
    }


    public List<Integer> Find_Gap_Partitions(){
        List<Integer> Partitions = new ArrayList<>();
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            if(PeerData.Partition_Availability.get(i).size() == 0){
                Partitions.add(i);
            }
        }
        return Partitions;
    }

    //These function checks if already existing peers have sent their
    // authority partitions. If it is true then returns true else return
    // false
    public boolean received_from_all_peers() throws IOException {
        int i,size;
        List<String> Peers_id = new ArrayList<String>();
        List<Peer> Peers = ipfs.swarm.peers();
        //Update existing peers list in case some peers left
        for(i = 0; i < Peers.size(); i++){
            Peers_id.add(Peers.get(i).id.toString());
        }
        i = 0;
        size = PeerData.Existing_peers.size();
        //Remove non existing peers
        while(size <= i){
            if(Peers_id.contains(PeerData.Existing_peers.get(i)) == false){
                PeerData.Existing_peers.remove(i);
                size--;
            }
            else{
                i++;
            }
        }
        //Check collected authorities
        for(i = 0; i < PeerData.Existing_peers.size(); i++){
            if(PeerData.Swarm_Peer_Auth.containsKey(PeerData.Existing_peers.get(i)) == false){
                return false;
            }
        }
        return  true;
    }



    public void select_partition() throws Exception {
        int i,new_key,rand,max_loaded= 0;
        List<Integer> Peer_Auth = new ArrayList<Integer>();
        List<Peer> Peers = ipfs.swarm.peers();

        Map<Integer,Integer> Partition_Cardinality = new HashMap<Integer, Integer>();
        String Loaded_Peer = null;
        Random Randint = new Random();

        // Check if there an overloaded peer in the swarm

        for(i = 0; i < Peers.size(); i++){
           // System.out.println(PeerData.Swarm_Peer_Auth);
            //System.out.println(PeerData.Swarm_Peer_Auth.get(Peers.get(i).id.toString()).size());
            if( PeerData.Swarm_Peer_Auth.containsKey(Peers.get(i).id.toString()) && PeerData.Swarm_Peer_Auth.get(Peers.get(i).id.toString()).size()>_MIN_PARTITIONS){
                if(PeerData.Swarm_Peer_Auth.get(Peers.get(i).id.toString()).size()>max_loaded){
                    Loaded_Peer = Peers.get(i).id.toString();
                    max_loaded = PeerData.Swarm_Peer_Auth.get(Peers.get(i).id.toString()).size();
                }
            }
        }
        //If overloaded peer exists select partitions and return
        if(max_loaded !=0){
            int[] Auth = new int[_MIN_PARTITIONS];
            Peer_Auth =  PeerData.Swarm_Peer_Auth.get(Loaded_Peer);
            rand = Randint.nextInt(Peer_Auth.size());
            for(i = 0; i < _MIN_PARTITIONS; i++){
                PeerData.Auth_List.add(Peer_Auth.get((i + rand)%Peer_Auth.size()));

            }
            System.out.println(PeerData.Auth_List);
            //PUBLISH
            System.out.println("======");
            System.out.println(Loaded_Peer);
            System.out.println(ipfs.id().get("ID").toString());
            System.out.println("=========");
            ipfs.pubsub.pub("Authorities",ipfsClass.Marshall_Packet(PeerData.Auth_List,Loaded_Peer,ipfs.id().get("ID").toString(),(short)1));

            return;
        }

        //Else select least frequently used partition
        for(i = 0; i < _PARTITIONS; i++){
            Partition_Cardinality.put(i,PeerData.Partition_Availability.get(i).size());
        }

        for(i = 0; i < _MIN_PARTITIONS; i++){
            new_key = Collections.min(Partition_Cardinality.entrySet(),Comparator.comparing(Map.Entry::getValue)).getKey();
            PeerData.Auth_List.add(new_key);
            PeerData.Partition_Availability.remove(new_key);
        }
        ipfs.pubsub.pub("Authorities",ipfsClass.Marshall_Packet(PeerData.Auth_List,null,ipfs.id().get("ID").toString(),(short) 2));

    }



    //Partition Gradient vector into a set of smaller vectors corespoding to a partition ID
    public Map<Integer,List<Double>> OrganizeGradients(List<Double> Gradients){
        int i,j,chunk_size = (int)(PeerData._MODEL_SIZE/PeerData._PARTITIONS) +1;
        Map<Integer,List<Double>> Partition = new HashMap<Integer, List<Double>>();

        for(i = 0; i < PeerData._PARTITIONS; i++){
            List<Double> Gradient_Partition = new ArrayList<Double>();
            for(j = i*chunk_size; j < (i+1)*chunk_size && j < Gradients.size(); j++){
                Gradient_Partition.add(Gradients.get(j));
            }
            Partition.put(i,Gradient_Partition);
        }
        return Partition;

    }


    public int GetIterClock(){
        return PeerData._Iter_Clock;
    }


    public List<Double> GetPartitions() throws IOException, ClassNotFoundException {
        int i,j;
        List<Double> Parameters = new ArrayList<>();

        if(PeerData.First_Iter){
            for(i = 0; i < PeerData._PARTITIONS; i++){
                for(j = 0; j < PeerData.Weights.get(i).size(); j++){
                    Parameters.add(PeerData.Weights.get(i).get(j));
                }
            }
            PeerData.First_Iter = false;
            return Parameters;
        }

        // Check if Updated Partition already does not exist locally
        for(i = 0; i < PeerData._PARTITIONS; i++){
            if(PeerData.Auth_List.contains(i) == false && PeerData.Weight_Address.containsKey(i)){
                //System.out.println("DOWNLOADING : " +  PeerData.Weight_Address.get(i));
                System.out.println("Getting From remote peers");
                PeerData.Weights.put(i,PeerData.Weight_Address.get(i));
            }
        }
        // Create a parameter vector
        for(i = 0; i < PeerData._PARTITIONS; i++){
            for(j = 0; j < PeerData.Weights.get(i).size(); j++){
                Parameters.add(PeerData.Weights.get(i).get(j));
            }
        }

        return Parameters;

    }

    //Before Start trainning any model we have to get updated weights, so we
    // ask a peer to provide us a hash where weights are stored
    public void LoadModel() throws Exception {
        String Peer = null;

        for(int i = 0; i < PeerData._PARTITIONS; i++){
            if(PeerData.Partition_Availability.get(i).size() != 0){
                Peer = PeerData.Partition_Availability.get(i).get(0);
                break;
            }
        }

        System.out.println(Peer);
        if(Peer == null){
            System.out.println("An error occurred on loading the model. Exiting...");
        }
        while (true) {
            //Get first peer in list
            //System.out.println(PeerData.Existing_peers);

            org.javatuples.Pair<String,Integer> pair = new org.javatuples.Pair(Peer, 0);

            PeerData.Wait_Ack.add(pair);
            ipfs.pubsub.pub(Peer, ipfsClass.Marshall_Packet(ipfs.id().get("ID").toString(), (short) 5));
            //Wait until received the request
            while (PeerData.Wait_Ack.size() != 0) { Thread.yield();}
            if(PeerData.Weights.get(0).size() != 0){
                break;
            }
        }
    }
    public void LoadModel(String Peer) throws Exception {
        org.javatuples.Pair<String,Integer> pair = new org.javatuples.Pair(Peer, 0);

        while (true) {

            //Get first peer in list
            PeerData.Wait_Ack.add(pair);
            ipfs.pubsub.pub(Peer, ipfsClass.Marshall_Packet(ipfs.id().get("ID").toString(), (short) 5));
            //Wait until received the request
            while (PeerData.Wait_Ack.size() != 0) { Thread.yield();}
            if(PeerData.Weights.get(0).size() != 0){
                break;
            }
        }
    }
    //Update Gradient is an API method, where after each iteration of the
    // Learning phase we send the updated gradients to other peers
    public void UpdateGradient(List<Double> Gradients) throws Exception {
        int i;
        List<Integer> Partitions = new ArrayList<Integer>();
        String Peer = null;
        Map<Integer,List<Double>> GradientPartitions = OrganizeGradients(Gradients);
        org.javatuples.Pair<String,Integer> tuple;

        for(i = 0; i < _PARTITIONS; i++){
            Partitions.add(i);
        }
        for(i = 0; i < PeerData.Auth_List.size(); i++){
            Partitions.remove((Integer) PeerData.Auth_List.get(i));
            //Put the request to the Updater
            System.out.println("PUTING GRADIENTS : " +  PeerData.Auth_List.get(i));

            PeerData.queue.add(new Triplet<String, Integer, List<Double>>(ipfs.id().get("ID").toString(),PeerData.Auth_List.get(i),GradientPartitions.get(PeerData.Auth_List.get(i))));
        }
        for(i = 0; i < Partitions.size(); i++){
            if(PeerData.Partition_Availability.get(Partitions.get(i)).size() == 0){
                // Crash handlinh
            }
            else{
                Peer = PeerData.Partition_Availability.get(Partitions.get(i)).get(0);
                tuple = new org.javatuples.Pair<>(Peer,Partitions.get(i));
                PeerData.Wait_Ack.add(tuple);
                ipfs.pubsub.pub(Peer,ipfsClass.Marshall_Packet(GradientPartitions.get(i),ipfs.id().get("ID").toString(),Partitions.get(i),(short) 3));
                //SEND GRADIENTS
                System.out.println("SEND GRADIENTS : " + Partitions.get(i));
            }
        }
        PeerData.Gradients = Gradients;
        PeerData.sendingGradients = true;
        while(PeerData.Wait_Ack.size() != 0){Thread.yield();}
        PeerData.sendingGradients = false;
        System.out.println("Gradient Sending Completed ");
        PeerData._Iter_Clock++;


    }


    public void InitializeWeights(){
        int i;
        for(i = 0; i < PeerData._PARTITIONS; i++){
            PeerData.Weights.put(i,new ArrayList<>());
            PeerData.Aggregated_Gradients.put(i,new ArrayList<>());
        }
    }

    public void InitializeWeights(List<Double> Model){
        int i,j,chunk_size = (int)(PeerData._MODEL_SIZE/PeerData._PARTITIONS) +1;
        for(i = 0; i < PeerData._PARTITIONS; i++){
            for(j = i*chunk_size; j < (i+1)*chunk_size && j < Model.size(); j++){
                PeerData.Weights.get(i).add(Model.get(j));
                PeerData.Aggregated_Gradients.get(i).add(0.0);
            }
        }
    }


    //Main will become init method for the IPLS API
    //Here the peer will first interact with the other
    //peers in the swarm and take the responsibilities
    public void init(String path,List<String> BootstrampPeers,boolean bootstraper) throws Exception {
        int i = 0;
        //This is a list showing which partitions are missing from PeerData
        // in the initialization phase, in order to put them in our authority
        // list
        List<Integer> GapPartitions = new ArrayList<>();
        org.javatuples.Pair<String,Integer> tuple;
        PeerData.Bootstrapers = BootstrampPeers;
        PeerData.Path = path;
        PeerData._MODEL_SIZE = 443610;
        InitializeWeights();
        for (i = 0; i < _PARTITIONS; i++) {
            PeerData.Partition_Availability.put(i,new ArrayList<>());
        }
        //Each peer gets in init phase an authority list in which he
        // subscribes in order to get gradients
        //List<Integer> Auth_List = new ArrayList<Integer>();
        List<String> BootstrapRequest = new ArrayList<String>();
        List<Peer> peers = new ArrayList<Peer>();


        ipfsClass = new MyIPFSClass(path);
        ipfs = new IPFS(path);

        PeerData._ID = ipfs.id().get("ID").toString();
        
        
        
        
        //==============================//

        try {
            peers = ipfs.swarm.peers();
            for(i = 0; i < peers.size(); i++){
                PeerData.Existing_peers.add(peers.get(i).id.toString());
            }
        }
        catch(Exception e){
            peers = null;
        }

        //This is probably going to change and data might be downloaded from ethereum
        //For now we pick the weights from a file
        //****************************************************//

        FileInputStream fin = new FileInputStream("ETHModel");
        ObjectInputStream oin = new ObjectInputStream(fin);

        List<Double> Lmodel = (List<Double>)oin.readObject();
        PeerData._MODEL_SIZE = Lmodel.size();
        InitializeWeights(Lmodel);

        //****************************************************//
        //Start _New_Peer thread in order to get new_peer messages
        _New_Peer = new ThreadReceiver(path);
        _New_Peer.start();
        PeerData.InitSem.acquire();
        
        //Start Personal_Thread to get personal messages
        _Personal_Thread = new ThreadReceiver(ipfs.id().get("ID").toString(),path,_PARTITIONS,_MIN_PARTITIONS);
        _Personal_Thread.start();
        PeerData.InitSem.acquire();
        
        _Auth_Listener = new ThreadReceiver("Authorities",path,_PARTITIONS,_MIN_PARTITIONS);
      
        _Auth_Listener.start();
        PeerData.InitSem.acquire();
        Thread.sleep(1000);
        
        
        
        if(bootstraper == true){
            Sub thread;
            for(i = 0; i < PeerData._PARTITIONS; i++) {
                thread = new Sub(new Integer(i).toString(),PeerData.Path,PeerData.GGP_queue,true);
                thread.start();
            }
            return;
        }

        if(peers == null){
            System.out.println("Error, IPLS could not find any peers. Aborting ...");
            System.exit(-1);
        }
        System.out.println(peers);

        //First, send a request to bootstrapers and ask them to give
        // you peer info in order to connect them into your swarm
        BootstrapRequest.add(ipfs.id().get("ID").toString());
        for(i = 0; i < peers.size(); i++){
            if(BootstrampPeers.contains(PeerData.Existing_peers.get(i))){
                tuple = new org.javatuples.Pair<>(PeerData.Existing_peers.get(i),0);
                PeerData.Wait_Ack.add(tuple);
                System.out.println(PeerData.Existing_peers.get(i));
                ipfs.pubsub.pub(PeerData.Existing_peers.get(i),ipfsClass.Marshall_Packet(BootstrapRequest,false));
                System.out.println("Sent");
            }
        }
        while(PeerData.Wait_Ack.size() != 0){Thread.yield();}

        BootstrapRequest.add(PeerData.MyPublic_Multiaddr);

        System.out.println("Bootstrap Request : "  + BootstrapRequest);
        ipfs.pubsub.pub("New_Peer",ipfsClass.Marshall_Packet(BootstrapRequest,false));




        if(peers != null){
            for(i = 0; i < 2; i++){
                Thread.sleep(2000);
                if(received_from_all_peers()){
                    break;
                }
                System.out.println("Waiting Another 2sec");
            }
            GapPartitions = Find_Gap_Partitions();
            System.out.println(GapPartitions);
            if(GapPartitions.size() != 0){
                System.out.println("GAP DETECTED : " + GapPartitions );
                //In case we have concurrent joins
                for(i = 0; i < GapPartitions.size(); i++){
                    PeerData.Auth_List.add(GapPartitions.get(i));
                }
                ipfs.pubsub.pub("Authorities",ipfsClass.Marshall_Packet(PeerData.Auth_List,null,ipfs.id().get("ID").toString(),(short) 2));
            }
            else{
                select_partition();
                LoadModel();
            }
        }
        else{
            for(i = 0; i < _PARTITIONS; i++){
                PeerData.Auth_List.add(i);
            }
        }
        SwarmManagerThread = new SwarmManager();
        SwarmManagerThread.start();
        System.out.println("Swarm Manager Started...");
        UpdaterThread = new Updater();
        UpdaterThread.start();
        System.out.println("Updater Started...");

        //----------------------------------------------------//
        List<Double> VectorModel = new ArrayList<>();
        for(i = 0; i < PeerData._PARTITIONS; i++){
            for(int j = 0; j < PeerData.Weights.get(i).size(); j++){
                VectorModel.add(PeerData.Weights.get(i).get(j));
            }
        }
        File f = new File(ipfs.id().get("ID").toString()+"ETHModel");
        f.createNewFile();
        FileOutputStream fos = new FileOutputStream(ipfs.id().get("ID").toString()+"ETHModel");
        ObjectOutputStream oos = new ObjectOutputStream(fos);

        oos.writeObject(VectorModel);
        oos.close();
        fos.close();

        GlobalGradientPool GP_Thread = new GlobalGradientPool();
        GP_Thread.start();

    }
}
