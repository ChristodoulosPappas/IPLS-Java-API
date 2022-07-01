import io.ipfs.api.*;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.apache.commons.math3.analysis.function.Add;
//import org.bytedeco.opencv.presets.opencv_core;
import org.javatuples.*;

import java.awt.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.javatuples.Pair;
import org.json.JSONObject;
import org.nd4j.linalg.api.ops.custom.Lu;
import org.web3j.abi.datatypes.Int;
import org.web3j.protocol.core.methods.response.EthLog;

import javax.crypto.SecretKey;
//import sun.nio.ch.sctp.PeerAddrChange;

class ThreadReceiver extends Thread{
    int _PARTITIONS,_MIN_PARTITIONS;
    String Topic,_ID,Path;
    IPFS ipfs;
    MyIPFSClass AuxilaryIpfs = new MyIPFSClass(PeerData.Path);
    Bootstraper_Services schedule_daemon = null;
    IPLS_Comm commit ;

    public ThreadReceiver(String path){
        Path = path;
        ipfs = new IPFS(path);
        Topic = "New_Peer";
        commit = new IPLS_Comm(path);
    }

    public ThreadReceiver(String topic,String  path,int PARTITIONS,int MIN_PARTITIONS){
        Path = path;
        ipfs = new IPFS(path);
        commit = new IPLS_Comm(path);
        Topic = topic;
        _PARTITIONS = PARTITIONS;
        _MIN_PARTITIONS = MIN_PARTITIONS;
    }

    //This function checks if peer belongs into peer's swarm
    public boolean _In_swarm(String PeerId){
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
        int i;
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
            int[] Auth = new int[_MIN_PARTITIONS/2];
            rand = Randint.nextInt(PeerData.Auth_List.size());
            for(i = 0; i < (int)(PeerData.Auth_List.size()/2); i++){
                Auth[i] = PeerData.Auth_List.get((i + rand)%PeerData.Auth_List.size());
            }
            return Auth;

        }

    }

    //Inform peers about discharge of a responsibility or removal of a node
    public void Inform_Peers(int partition) throws Exception {
        if(!PeerData.isSynchronous){
            return;
        }

        for(int j=  0; j < PeerData.Clients.get(partition).size(); j++){
            AuxilaryIpfs.send(PeerData.Clients.get(partition).get(j),AuxilaryIpfs.JOIN_PARTITION_SERVER(PeerData._ID,partition,(short)1));
        }
        for(int j = 0; j < PeerData.New_Clients.get(partition).size(); j++){
            AuxilaryIpfs.send(PeerData.New_Clients.get(partition).get(j),AuxilaryIpfs.JOIN_PARTITION_SERVER(PeerData._ID,partition,(short)1));
        }
        //Inform all other replicas
        AuxilaryIpfs.send(String.valueOf(partition),AuxilaryIpfs.JOIN_PARTITION_SERVER(PeerData._ID,partition,(short)1));
    }

    //This function is called when an overloaded peer is able to remove
    // partitions that another peer wanted to have.
    public void Remove_Partitions(List<Integer> Peer_Auth) throws Exception {
        int i,remove_limit,counter = 0;
        PeerData.dlog.log("REMOVING PARTITIONS!!! :( : " + Peer_Auth);
        if(PeerData.Auth_List.size() - Peer_Auth.size() >= _MIN_PARTITIONS){
            remove_limit = Peer_Auth.size();
            for(i = 0; i < remove_limit; i++){
                if(PeerData.Auth_List.contains(Integer.valueOf(Peer_Auth.get(i)))){
                    PeerData.Auth_List.remove(Integer.valueOf(Peer_Auth.get(i)));

                    Inform_Peers(Peer_Auth.get(i));
                    for(int j = 0; PeerData.Clients.get(Peer_Auth.get(i)) != null && j < PeerData.Clients.get(Peer_Auth.get(i)).size(); j++){
                        PeerData.Client_Wait_Ack.remove(new Triplet<>(PeerData.Clients.get(Peer_Auth.get(i)).get(j),Peer_Auth.get(i),PeerData.middleware_iteration));
                        PeerData.Client_Wait_Ack.remove(new Triplet<>(PeerData.Clients.get(Peer_Auth.get(i)).get(j),Peer_Auth.get(i),PeerData.middleware_iteration+1));
                    }
                    for(int j = 0; PeerData.Replica_holders.get(Peer_Auth.get(i)) != null && j < PeerData.Replica_holders.get(Peer_Auth.get(i)).size(); j++){
                        PeerData.Replica_Wait_Ack.remove(new Triplet<>(PeerData.Replica_holders.get(Peer_Auth.get(i)).get(j),Peer_Auth.get(i),PeerData.middleware_iteration));
                        PeerData.Replica_Wait_Ack.remove(new Triplet<>(PeerData.Replica_holders.get(Peer_Auth.get(i)).get(j),Peer_Auth.get(i),PeerData.middleware_iteration + 1));
                    }
                    PeerData.New_Replicas.put(Integer.valueOf(Peer_Auth.get(i)),new ArrayList<>());
                    PeerData.Replica_holders.put(Integer.valueOf(Peer_Auth.get(i)),new ArrayList<>());
                    PeerData.Clients.put(Integer.valueOf(Peer_Auth.get(i)), new ArrayList<>());
                    PeerData.New_Clients.put(Integer.valueOf(Peer_Auth.get(i)),new ArrayList<>());
                    PeerData.UpdateQueue.add(new org.javatuples.Pair<>(0,Peer_Auth.get(i)));

                    AuxilaryIpfs.send("New_Peer",AuxilaryIpfs.Marshall_Packet(
                            AuxilaryIpfs._Upload_File(PeerData.Weights.get(Peer_Auth.get(i)), "IPLS_directory_" + PeerData._ID + "/" + Peer_Auth.add(i) + "_Updates").toString(),
                            PeerData._ID,
                            Peer_Auth.get(i),
                            (short) 12));
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
                    Inform_Peers(Peer_Auth.get(i));
                    for(int j = 0; PeerData.Clients.get(Peer_Auth.get(i)) != null && j < PeerData.Clients.get(Peer_Auth.get(i)).size(); j++){
                        PeerData.Client_Wait_Ack.remove(new Triplet<>(PeerData.Clients.get(Peer_Auth.get(i)).get(j),Peer_Auth.get(i),PeerData.middleware_iteration));
                    }
                    PeerData.Clients.put(Integer.valueOf(Peer_Auth.get(i)), new ArrayList<>());
                    PeerData.UpdateQueue.add(new org.javatuples.Pair<>(0,Peer_Auth.get(i)));
                    PeerData.dlog.log("SEND TASK");
                    counter++;
                    if(counter == remove_limit){
                        break;
                    }
                }
            }
        }
    }

    //THIS FUNCTION SHOULD BE EXECUTED IN ATOMIC MANNER
    public  void update_peer_structures(short pid,String Peer_Id, List<Integer> Peer_Auth,String renting_peer) throws Exception {
        int i;
        String packet = null;
        List<String> Peers_id = new ArrayList<String>();
        List<Peer> Peers = null;
        try {
            Peers = ipfs.swarm.peers();
        }
        catch(Exception e){
            PeerData.dlog.log("Unable to find peers");
        }
        //In case remote peer adds in its authority list
        // Partitions we should add those in our data structures
        if(pid == 1 || pid == 2) {
            //Update existing peers list in case some peers left
            //Create String List with peer hash IDs
            for (i = 0; i < Peers.size(); i++) {
                Peers_id.add(Peers.get(i).id.toString());
            }

            //If the peer that is in my swarm and it is not a heartbeat message continue
            if ((PeerData.Swarm_Peer_Auth.get(Peer_Id) == null || !PeerData.Swarm_Peer_Auth.get(Peer_Id).equals(Peer_Auth))) {
                PeerData.mtx.acquire();

                //First remove old partitions of the peer
                if(PeerData.Swarm_Peer_Auth.get(Peer_Id) != null ){
                    for (i = 0; i < PeerData.Swarm_Peer_Auth.get(Peer_Id).size(); i++) {
                        if(!Peer_Auth.contains(PeerData.Swarm_Peer_Auth.get(Peer_Id).get(i))){
                            PeerData.Partition_Availability.get(PeerData.Swarm_Peer_Auth.get(Peer_Id).get(i)).remove(Peer_Id);
                        }
                    }
                    //Add in new Peer partitions its Peer id
                    for (i = 0; i < Peer_Auth.size(); i++) {
                        if(!PeerData.Swarm_Peer_Auth.get(Peer_Id).contains(Peer_Auth.get(i))){
                            PeerData.Partition_Availability.get(Peer_Auth.get(i)).add(Peer_Id);
                        }
                    }
                }
                else{
                    for (i = 0; i < Peer_Auth.size(); i++) {
                        PeerData.Partition_Availability.get(Peer_Auth.get(i)).add(Peer_Id);
                    }
                }

                PeerData.Swarm_Peer_Auth.put(Peer_Id, Peer_Auth);

                //In case remote peer believes that peer is overloaded
                // peer must check if he actually is and then remove some
                // number of partitions and then notifies peers about its
                // new state
                if (renting_peer != null && renting_peer.equals(_ID)) {
                    PeerData.dlog.log("Removing Partitions");
                    if (PeerData.Auth_List.size() > _MIN_PARTITIONS) {
                        Remove_Partitions(Peer_Auth);
                        try{
                            packet = AuxilaryIpfs.Marshall_Packet(PeerData.Auth_List, null, ipfs.id().get("ID").toString() ,(short) 2);
                        }
                        catch (Exception e){
                            e.printStackTrace();
                        }
                        if (packet != null) {
                            try{
                                AuxilaryIpfs.send("Authorities", packet);
                                //Send to all peers cancelation message
                            }
                            catch (Exception e){
                                PeerData.dlog.log("Unable for pub/sub");
                            }
                        }
                    }
                }
                PeerData.mtx.release();

            }
        }
    }




    public void connect(String Addr)throws IOException{
        String[] addr = Addr.split("/");
        try {
        	ipfs.swarm.connect(new MultiAddress(Addr.replace("p2p","ipfs")));
        	PeerData.dlog.log("Connected succesfully to a peer : " + Addr);
        	if(PeerData.Existing_peers.contains(addr[addr.length-1]) == false){
        	    PeerData.Existing_peers.add(addr[addr.length-1]);
            }
        }
        catch (Exception e){
        	PeerData.dlog.log("Unable to connect to peer : " + Addr);
        }
        addr = null;
    }

    public void HANDLE_DISCHARGE(int partition,String Discharged_Peer) throws Exception{
        // Remove the dealer and then remove him from the wait ack list in order
        // to proceed to the next iteration where you are going to select new peer
        PeerData.dlog.log("PEER DISCARDED A RESPONSIBILITY " + Discharged_Peer + " , "+ partition);

        PeerData.mtx.acquire();
        PeerData.Dealers.remove(partition);
        if(PeerData.First_Iter){
            PeerData.Wait_Ack.remove(new Triplet<>(Discharged_Peer,partition,-1));

        }
        else{
            PeerData.Wait_Ack.remove(new Triplet<>(Discharged_Peer,partition,PeerData.middleware_iteration));
        }
        PeerData.mtx.release();
    }

    public void HANDLE_JOIN_REQUEST(int partition, int peer_clock, String new_client, int is_reply) throws  Exception{
        PeerData.dlog.log("PEER WANTS TO JOIN FOR PARTITION : " + partition + " , " + PeerData.Auth_List);
        if(PeerData.Auth_List.contains(partition)){
            PeerData.mtx.acquire();
            //If peer is indeed responsible for the requested partition, send him back the weights
            //send weights
            PeerData.dlog.log(">> NEW CLIENT " + new_client );
            // In case the peer is new member then add it to new members and send updates
            PeerData.Clients_Iteration.put(new_client,peer_clock);

            // Get join request from a fresh member, send him an ACK in order to finish his initialization phase.
            if(is_reply == 0){
                //Auxiliary.send(new_client,AuxilaryIpfs.Marshall_Packet(PeerData.Weights.get(partition),ipfs.id().get("ID").toString(),partition,PeerData.middleware_iteration,(short)4));
                PeerData.New_Members.get(partition).add(new_client);
                if(AuxilaryIpfs.find_iter() == -1 || AuxilaryIpfs.get_curr_time() < AuxilaryIpfs.training_elapse_time(AuxilaryIpfs.find_iter())){
                    PeerData.Clients.get(partition).add(new_client);
                    PeerData.Client_Wait_Ack.add(new Triplet<>(new_client,partition,(AuxilaryIpfs.find_iter() == -1)?PeerData.middleware_iteration+1:AuxilaryIpfs.find_iter()));
                }
                else{
                    PeerData.New_Clients.get(partition).add(new_client);
                }
            }
            else if(is_reply == 3){
                if(AuxilaryIpfs.training_elapse_time(AuxilaryIpfs.find_iter()) > AuxilaryIpfs.get_curr_time()){
                    if(!PeerData.Replica_holders.get(partition).contains(new_client)){
                        PeerData.Replica_holders.get(partition).add(new_client);
                        PeerData.Replica_Wait_Ack.add(new Triplet<>(new_client,partition,AuxilaryIpfs.find_iter()));
                    }
                }
                else if(!PeerData.New_Replicas.get(partition).contains(new_client) && !PeerData.Replica_holders.get(partition).contains(new_client)){
                    PeerData.New_Replicas.get(partition).add(new_client);
                }
            }
            else{
                // If i am a new member then put everything in hold back queue in order to process it later. The process will take place on
                // My first pseudo round where i will clear up everything.
                if(PeerData.First_Iter){
                    PeerData.dlog.log("FIRST ITER");
                    PeerData.Client_Wait_Ack_from_future.add(new Triplet<>(new_client,partition,peer_clock));
                    PeerData.New_Clients.get(partition).add(new_client);
                }
                else{
                    // If there is a peer ahead (one iteration further), then put it in hold back queue
                    if(PeerData.middleware_iteration < peer_clock ){
                        PeerData.Client_Wait_Ack_from_future.add(new Triplet<>(new_client,partition,peer_clock));
                        PeerData.New_Clients.get(partition).add(new_client);
                    }
                    // Else if there is a peer in the same iteration just inform him now or later
                    else if(PeerData.middleware_iteration == peer_clock){
                        //if(PeerData.Client_Wait_Ack.size() > 0) {
                        //    PeerData.Clients.get(partition).add(new_client);
                        //}
                        //else{
                        AuxilaryIpfs.send(new_client,AuxilaryIpfs.Marshall_Packet(PeerData.Weights.get(partition),ipfs.id().get("ID").toString(),partition,PeerData.middleware_iteration,(short)4));
                        PeerData.New_Clients.get(partition).add(new_client);
                        //}
                    }
                    else{
                        AuxilaryIpfs.send(new_client,AuxilaryIpfs.Marshall_Packet(PeerData.Weights.get(partition),ipfs.id().get("ID").toString(),partition,PeerData.middleware_iteration,(short)4));
                        PeerData.New_Clients.get(partition).add(new_client);
                    }
                }
            }
            //PeerData.Client_Wait_Ack.add(new Pair<String,Integer>(ReplyPair.getValue0(),ReplyPair.getValue1()));
            PeerData.mtx.release();
        }
        else{
            //In case the peer is not responsible for the partition any more, then send an ACK
            AuxilaryIpfs.send(new_client,AuxilaryIpfs.JOIN_PARTITION_SERVER(PeerData._ID,partition,(short) 1));
        }

    }

    public void HANDLE_CLIENT_DISCHARGE(String client) throws Exception{
        PeerData.mtx.acquire();

        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            if(PeerData.Clients.get(PeerData.Auth_List.get(i)).contains(client)){
                PeerData.Clients.get(PeerData.Auth_List.get(i)).remove(client);
                PeerData.Client_Wait_Ack.remove(new Triplet<>(client,PeerData.Auth_List.get(i),PeerData.middleware_iteration));
                PeerData.Client_Wait_Ack.remove(new Triplet<>(client,PeerData.Auth_List.get(i),PeerData.middleware_iteration+1));
            }
            else if(PeerData.New_Clients.get(PeerData.Auth_List.get(i)).contains(client)){
                PeerData.New_Clients.get(PeerData.Auth_List.get(i)).remove(client);
            }
        }

        PeerData.mtx.release();

    }

    public void HANDLE_REPLICA_DISCGARGE(String replica_holder) throws Exception{
        PeerData.mtx.acquire();

        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            if(PeerData.Replica_holders.get(PeerData.Auth_List.get(i)).contains(replica_holder)){
                PeerData.Replica_holders.get(PeerData.Auth_List.get(i)).remove(replica_holder);
                PeerData.Replica_Wait_Ack.remove(new Triplet<>(replica_holder,PeerData.Auth_List.get(i),PeerData.middleware_iteration));
                PeerData.Replica_Wait_Ack.remove(new Triplet<>(replica_holder,PeerData.Auth_List.get(i),PeerData.middleware_iteration+1));
            }
            else if(PeerData.New_Replicas.get(PeerData.Auth_List.get(i)).contains(replica_holder)){
                PeerData.New_Clients.get(PeerData.Auth_List.get(i)).remove(replica_holder);
            }
        }

        PeerData.mtx.release();

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
            //When topic = Authorities and pid == 1 then a peer wants to take
            // some rensponsibilities that the i have.
            if(pid == 1 && PeerData.Bootstrapers.contains(_ID) == false) {
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
            // When topic == Authorities and pid == 2 then it is just an information message
            // about the partitions a peer has
            else if(pid == 2 && PeerData.Bootstrapers.contains(_ID) == false){
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
            else if(Topic.equals(_ID) && (pid == 3 || pid == 23 || pid == 33) ){
                //String: Origin Peer , Integer : Partition, List<Double> : Gradients
                if(PeerData.Start_download_time == 0 && !PeerData.Indirect_Communication){
                    PeerData.Start_download_time = System.currentTimeMillis();
                }
                if(pid == 3){
                    Quartet<String,Integer,Integer,double[]> quartet = AuxilaryIpfs.GET_GRADIENTS(rbuff,bytes_array);
                    PeerData.downloaded_hashes++;
                    PeerData.Test_mtx.acquire();
                    PeerData.DataRecv += decodedString.length();
                    PeerData.Test_mtx.release();
                    PeerData.queue.add(new Sextet<>(new ArrayList(Collections.singleton(quartet.getValue0())),quartet.getValue1(),quartet.getValue2(),true,quartet.getValue3(),null));
                }
                else if(pid == 23){
                    //Receive commit <Partition,Iteration,Hash, origin_peer>
                    Quintet<Integer,Integer,String,String,String> Reply = AuxilaryIpfs.Get_Gradient_Commitment(rbuff,bytes_array);
                    //process commitment
                    commit.process_commitment(Reply.getValue0(),Reply.getValue3(),Reply.getValue2(),Reply.getValue1(),Reply.getValue4());
                }
                else{
                    // Receive Partition,iteration Hash, origin_peer and secret key

                    Quintet<Integer,Integer,String,String, SecretKey> Reply = AuxilaryIpfs.Get_SecretKey(rbuff,bytes_array);
                    //In case that the file is already downloaded, then just decrypt it and add it to the
                    // updater queue. Otherwise add the key to Hash_keys in order for the file to be decrypted
                    // whenever it is downloaded
                    PeerData.com_mtx.acquire();
                    if(PeerData.Downloaded_Hashes.containsKey(Reply.getValue2())){
                    //    PeerData.queue.add(new Sextet<>(new ArrayList<>(Collections.singleton(Reply.getValue3())),Reply.getValue0(),Reply.getValue1(),true,(List<Double>) AuxilaryIpfs.decrypt(AuxilaryIpfs.Get_bytes(Reply.getValue2()),Reply.getValue4()),null));
                        PeerData.Downloaded_Hashes.remove(Reply.getValue2());
                    }
                    else{
                        PeerData.Hash_Keys.put(Reply.getValue2(),Reply.getValue4());
                    }
                    PeerData.com_mtx.release();

                }
            }
            // Get ACK message
            else if(Topic.equals(_ID) && (pid == 4 || pid == 24)){
                if(pid == 4){
                    // PeerId , Partition , iteration , Gradients
                    Quartet<String,Integer,Integer,double[]> ReplyPair = AuxilaryIpfs.GET_GRADIENTS(rbuff,bytes_array);
                    org.javatuples.Triplet<String,Integer,Integer> pair = new org.javatuples.Triplet<>(ReplyPair.getValue0(),ReplyPair.getValue1(),ReplyPair.getValue2());
                    for(i = 0; i < PeerData.Weight_Address.get(ReplyPair.getValue1()).length; i++){
                        PeerData.Weight_Address.get(ReplyPair.getValue1())[i]  = ReplyPair.getValue3()[i];
                    }
                    //PeerData.Weight_Address.put(ReplyPair.getValue1(),ReplyPair.getValue2());
                    PeerData.mtx.acquire();

                    // Check the iteration number of each server. Each peer in the system must be synchronized
                    // But by some reason some peers may meet you in iteration n and others in iteration n+1
                    if(PeerData.Servers_Iteration.containsKey(ReplyPair.getValue0())){
                        if(PeerData.Servers_Iteration.get(ReplyPair.getValue0()) < ReplyPair.getValue2()){
                            PeerData.Servers_Iteration.put(ReplyPair.getValue0(),ReplyPair.getValue2());
                        }
                    }
                    else{
                        PeerData.Servers_Iteration.put(ReplyPair.getValue0(),ReplyPair.getValue2());
                    }
                    //if(PeerData.middleware_iteration > 0 && ReplyPair.getValue2() == 0){
                    //    ipfs.pubsub.pub(ReplyPair.getValue0(),AuxilaryIpfs.Marshall_Packet(PeerData.Weights.get(ReplyPair.getValue1()),ipfs.id().get("ID").toString(),ReplyPair.getValue1(),PeerData.middleware_iteration,(short)3));
                    //}
                    if (PeerData.Wait_Ack.contains(pair)) {
                        PeerData.Wait_Ack.remove(pair);

                    }
                    else if(PeerData.Wait_Ack.contains(new Triplet<>(ReplyPair.getValue0(),ReplyPair.getValue1(), PeerData.middleware_iteration))){
                        if(PeerData.middleware_iteration < ReplyPair.getValue2()){
                            PeerData.Wait_Ack.remove(new Triplet<>(ReplyPair.getValue0(),ReplyPair.getValue1(), PeerData.middleware_iteration));
                            PeerData.Wait_Ack_from_future.add(pair);
                        }
                    }
                    else if(PeerData.Wait_Ack.contains(new Triplet<>(ReplyPair.getValue0(),ReplyPair.getValue1(),-1))){
                        PeerData.Wait_Ack.remove(new Triplet<>(ReplyPair.getValue0(),ReplyPair.getValue1(),-1));
                    }
                    else {
                        PeerData.Wait_Ack_from_future.add(pair);
                    }
                    PeerData.mtx.release();

                    pair = null;
                    ReplyPair = null;

                }
                else{
                    //Receive commit <Partition,Iteration,Hash, origin_peer>
                    Quartet<Integer,Integer,String,String> Reply = AuxilaryIpfs.Get_Commitment(rbuff,bytes_array);
                    PeerData.updates_download_scheduler.add_update(new Quartet<>(Reply.getValue2(),Reply.getValue3(),Reply.getValue1(),Reply.getValue0()));
                    //Download_Scheduler download = new Download_Scheduler(Reply.getValue0(),Reply.getValue1(),Reply.getValue2(),Reply.getValue3());
                    //download.start();
                }

            }
            else if(Topic.equals(_ID) && pid == 5){
                int j;
                List<Double> Weights = new ArrayList<>();

                for(i = 0; i < PeerData._PARTITIONS; i++){
                    for(j = 0; j < PeerData.Weights.get(i).length; j++){
                        Weights.add(PeerData.Weights.get(i)[j]);
                    }
                }
                PeerId = AuxilaryIpfs.Get_Peer(rbuff,bytes_array,Short.BYTES);
                AuxilaryIpfs.send(PeerId,AuxilaryIpfs.Marshall_Packet(AuxilaryIpfs._Upload_File(Weights,_ID + "Model").toString(),(short) 6));
            }
            else if(Topic.equals(_ID) && pid == 6){
                int j,chunksize = (int)(PeerData._MODEL_SIZE/PeerData._PARTITIONS) + 1;


                PeerData._Iter_Clock = rbuff.getInt();
                String fileHash;
                List<Double> Parameters = new ArrayList<Double>();
                fileHash = AuxilaryIpfs.Get_Peer(rbuff,bytes_array,Short.BYTES + Integer.BYTES);
                Parameters = (List<Double>) AuxilaryIpfs.DownloadParameters(fileHash);
                if(Parameters.size() == PeerData._MODEL_SIZE){
                    for(i = 0; i < PeerData._PARTITIONS; i++){
                        for(j = i*chunksize; j < (i+1)*chunksize && j < Parameters.size(); j++){
                            PeerData.Weights.get(i)[i]  = Parameters.get(j);
                        }
                    }
                }
                PeerData.dlog.log("MODEL LOADED");
                PeerData.Wait_Ack.remove(0);

            }
            //This PID is called mainly on Bootstrapers, and is used
            // in order to inform the new peer about other peers
            else if(Topic.equals(_ID) && pid == 7){
                is_reply = rbuff.getShort();
                if(is_reply == 0){
                    PeerData.dlog.log("Bootstraper Request received");
                    List<String> Client = AuxilaryIpfs.Get_MultiaddrPeers(rbuff,bytes_array);
                    //PeerId = Get_Peer(rbuff,bytes_array,Short.BYTES);
                    List<Peer> Peer_Multiaddr = ipfs.swarm.peers();
                    List<String> Peers = new ArrayList<>();
                    for(i = 0; i < Peer_Multiaddr.size(); i++){
                        Peers.add(Peer_Multiaddr.get(i).address + "/" + "p2p/" + Peer_Multiaddr.get(i).id);
                    }
                    Peers.add(_ID);
                    if(PeerData.Schedule_Hash == null){
                        Peers.add("None");
                    }
                    else{
                        Peers.add(PeerData.Schedule_Hash);
                    }
                    AuxilaryIpfs.send(Client.get(0),AuxilaryIpfs.Marshall_Packet(Peers,true,(short) 7));

                }
                else if(is_reply == 1){
                    List<String> Multiaddr = AuxilaryIpfs.Get_MultiaddrPeers(rbuff,bytes_array);

                    AuxilaryIpfs.download_schedule(Multiaddr.get(Multiaddr.size()-1));

                    Multiaddr.remove(Multiaddr.size()-1);
                    PeerData.dlog.log("Multiaddr" + Multiaddr);
                    for(i = 0; i < Multiaddr.size()-1; i++){
                        String[] fractions = Multiaddr.get(i).split("/");
                        if(fractions[fractions.length-1].equals(_ID)){
                            PeerData.dlog.log("MY PUBLIC MULTI-ADDRESS : " +  Multiaddr.get(i));
                            PeerData.MyPublic_Multiaddr = Multiaddr.get(i);
                        }
                        else{
                            //try to connect to peer
                            connect(Multiaddr.get(i));
                        }
                    }
                    PeerData.Wait_Ack.remove(new Triplet<>(Multiaddr.get(Multiaddr.size()-1),0,0));
                }

            }
            else if(Topic.equals(_ID) && pid == 8){
                // String : Origin Peer, Integer : Partition
                is_reply = rbuff.getShort();
                org.javatuples.Triplet<String,Integer,Integer> ReplyPair = AuxilaryIpfs.Get_JoinRequest(rbuff,bytes_array);
                if(is_reply == 1 ){
                    HANDLE_DISCHARGE(ReplyPair.getValue1(),ReplyPair.getValue0());
                    return;
                }
                else{
                    HANDLE_JOIN_REQUEST(ReplyPair.getValue1(),ReplyPair.getValue2(),ReplyPair.getValue0(),is_reply);

                }

            }
            else if(Topic.equals(_ID) && pid == 9){
                PeerData.training_phase = true;
            }
            else if(Topic.equals(_ID) && pid == 19){
                Quintet<Short,String,Integer,Integer,List<String>> ReplyQuintet = AuxilaryIpfs.Merge_response(rbuff,bytes_array);
                if((int)ReplyQuintet.getValue0() != 1){
                    String Hash = ReplyQuintet.getValue4().get(0);
                    commit.process_commitment(ReplyQuintet.getValue2(),ReplyQuintet.getValue4(),Hash,ReplyQuintet.getValue3(),ReplyQuintet.getValue1(),(int)ReplyQuintet.getValue0());
                }
                else{

                    List<String> Hashes = ReplyQuintet.getValue4();
                    List<Double> Aggregation = (List<Double>)AuxilaryIpfs.DownloadParameters(Hashes.get(0));
                    List<Double> Gradient = new ArrayList<>();
                    for(i  =1; i < Hashes.size(); i++){
                        Gradient = (List<Double>) AuxilaryIpfs.DownloadParameters(Hashes.get(i));
                        for(int j = 0; j < Gradient.size(); j++){
                            Aggregation.set(j,Aggregation.get(j) + Gradient.get(j));
                        }
                    }
                    String Hash = commit.commit_partial_aggregation(Aggregation,ReplyQuintet.getValue3(),PeerData._ID);
                    Hashes.add(0,Hash);
                    AuxilaryIpfs.send(ReplyQuintet.getValue1(),AuxilaryIpfs.Marshall_Packet((short)0,ReplyQuintet.getValue2(),ReplyQuintet.getValue3(),ReplyQuintet.getValue1(),Hashes));
                }
            }
            else if(Topic.equals(_ID) && pid == 56){
                List<String> Store_Inform = AuxilaryIpfs.Get_Prov(rbuff,bytes_array);
                PeerData.Providers_Map.put(Store_Inform.get(0),Store_Inform.get(1));
            }

        }
        // Handle broadcast messages
        else if(Topic.equals("New_Peer")){
            // pid == 11 in case a peer left the system
            if(pid == 11 ){
            	if(PeerData.isBootsraper) {
            		return;
            	}
            	PeerData.SendMtx.acquire();
            	PeerData.mtx.acquire();
            	//packet is of the form : [Auths,[[strlen,peer,auth],...]]
                // When a peer (and specifically an aggregator) is leaving the system,
                // he selects some peers that will become responsible for the partitions
                // he was responsible for
                Map<Integer,String> SelectedPeers = AuxilaryIpfs.Get_RMap(rbuff,bytes_array);
                PeerData.dlog.log("SELECTED PEERS : " + SelectedPeers);
                
                List<Integer> ChangedList = new ArrayList<>();
                String LeavingPeer = SelectedPeers.get(-2);
                String fileHash = SelectedPeers.get(-1);
                int partition;
                boolean Authchanged = false;
                
                SelectedPeers.remove(-2);
                SelectedPeers.remove(-1);
                if(LeavingPeer.equals(PeerData._ID)) {
                	System.exit(1);
                }
                List<Integer> partitions = new ArrayList<>(SelectedPeers.keySet());




                for(i = 0; i < partitions.size(); i++){
                    partition = partitions.get(i);
                    if(SelectedPeers.get(partition).equals(PeerData._ID)) {
                        PeerData.dlog.log("Selected for responsibility of : " + partition);
                        if (!PeerData.Auth_List.contains(partition)) {
                            PeerData.Auth_List.add(partition);
                            PeerData.Clients.put(partition,new ArrayList<>());
                            PeerData.UpdateQueue.add(new org.javatuples.Pair<>(1,partition));
                            AuxilaryIpfs.send(String.valueOf(partition),AuxilaryIpfs.JOIN_PARTITION_SERVER(PeerData._ID,PeerData.Auth_List.get(i),(short)0));
                            Authchanged = true;
                        }
                        ChangedList.add(partition);
                    }
                    else{
                        if(!PeerData.isSynchronous) {
                            PeerData.Partition_Availability.get(partition).add(SelectedPeers.get(partition));
                            if (!PeerData.Swarm_Peer_Auth.containsKey(SelectedPeers.get(partition))) {
                                PeerData.Swarm_Peer_Auth.put(SelectedPeers.get(partition), new ArrayList<>());
                            }
                            PeerData.Swarm_Peer_Auth.get(SelectedPeers.get(partition)).add(partition);
                        }
                    }
                    PeerData.Partition_Availability.get(partition).remove(LeavingPeer);
                    PeerData.Swarm_Peer_Auth.remove(LeavingPeer);
                }
                if(Authchanged){
                    if(!PeerData.isSynchronous){
                        Map<Integer,double[]> parameters = AuxilaryIpfs.DownloadMapParameters(fileHash);
                        for(i = 0; i < ChangedList.size(); i++){
                            PeerData.queue.add(new Sextet<>(new ArrayList<>(Collections.singleton("LeavingPeer")),ChangedList.get(i),0,true,parameters.get(ChangedList.get(i)),null));
                        }
                    }
                    AuxilaryIpfs.send("Authorities",AuxilaryIpfs.Marshall_Packet(PeerData.Auth_List,null,ipfs.id().get("ID").toString(),(short) 2));
                }
                PeerData.SendMtx.release();
                PeerData.mtx.release();
                for(i = 0; i < partitions.size(); i++){
                    if(PeerData.Dealers.get(partitions.get(i)).equals(LeavingPeer)){
                        HANDLE_DISCHARGE(partitions.get(i),LeavingPeer);
                    }
                }
                HANDLE_REPLICA_DISCGARGE(LeavingPeer);
                HANDLE_CLIENT_DISCHARGE(LeavingPeer);
            }
            else if(pid == 9){
                PeerId = AuxilaryIpfs.Get_Peer(rbuff,bytes_array,Short.BYTES);
                if(!PeerData.Members.contains(PeerId) && PeerData.isBootsraper){
                    PeerData.Members.add(PeerId);
                    System.out.println("Adding member " + PeerData.Members.size());
                    if(PeerData.training_phase){
                        AuxilaryIpfs.send(PeerId,AuxilaryIpfs._START_TRAINING());
                    }
                    else if(PeerData.Members.size() ==  PeerData.Min_Members){
                        String[] Storage_nodes = PeerData.storage_client.get_Storage_View();
                        AuxilaryIpfs.update_file("Storage_View",Storage_nodes);
                        Multihash Hash = AuxilaryIpfs.add_file("Storage_View");
                        System.out.println("Storage View : " + Storage_nodes);
                        PeerData.training_phase = true;
                        if(schedule_daemon == null){
                            schedule_daemon = new Bootstraper_Services(PeerData.Path,"Scheduler",10,PeerData.Training_time,60,120,Hash.toString());
                            schedule_daemon.start();
                        }
                        for(i = 0; i < PeerData.Members.size(); i++){
                            AuxilaryIpfs.send(PeerData.Members.get(i),AuxilaryIpfs._START_TRAINING());
                        }

                    }
                }

            }
            else if(pid == 12){
                // data_hash -> (Partition ID, data hash, Origin id)
                Triplet<Integer,String,String> data_hash = AuxilaryIpfs.Get_data_hash(rbuff,bytes_array);
                if(PeerData.Auth_List.contains(data_hash.getValue0())){
                    PeerData.Hash_Partitions.put(data_hash.getValue0(),data_hash.getValue1());
                }
            }
            else if(pid == 15){
                // data_hash -> (Partition ID, data hash, Origin id)
                Triplet<Integer,String,String> data_hash = AuxilaryIpfs.Get_data_hash(rbuff,bytes_array);
                if(!PeerData.isBootsraper){
                    AuxilaryIpfs.download_schedule(data_hash.getValue1());
                    PeerData.dlog.log("DOWNLOADING SCHEDULE");
                }

            }
            else if(pid == 24){
                if(PeerData.isBootsraper){
                    return;
                }

                //Receive commit <Partition,Iteration,Hash, origin_peer>
                Quartet<Integer,Integer,String,String> Reply = AuxilaryIpfs.Get_Commitment(rbuff,bytes_array);
                // If peer is in his first iteration and needs to get the updated partitions
                // for those he is responsible for, puts them in Hash_Partitions.
                if(PeerData.First_Iter &&
                        PeerData.Auth_List.contains(Reply.getValue0()) &&
                        !PeerData.Hash_Partitions.containsKey(Reply.getValue0())){
                    PeerData.Hash_Partitions.put(Reply.getValue0(),Reply.getValue2());
                }
                if(!Reply.getValue3().equals(PeerData._ID) && !PeerData.Auth_List.contains(Reply.getValue0())){
                    PeerData.updates_download_scheduler.add_update(new Quartet<>(Reply.getValue2(),Reply.getValue3(),Reply.getValue1(),Reply.getValue0()));
                }
            }
            else {
                //useless short, (reply-request)
                rbuff.getShort();

                //Get peer data
                List<String> Peers = AuxilaryIpfs.Get_MultiaddrPeers(rbuff, bytes_array);
                PeerId = Peers.get(0);
                PeerData.dlog.log("New peer msg from : " + PeerId);
                if(PeerData.training_phase){
                    AuxilaryIpfs.send(PeerId,AuxilaryIpfs._START_TRAINING());
                }
                connect(Peers.get(1));
                if (_In_swarm(PeerId) && PeerId.equals(ipfs.id().get("ID").toString()) == false && PeerData.Bootstrapers.contains(_ID) == false) {
                    //try to put peer in your swarm if you can
                    reply = AuxilaryIpfs.Marshall_Packet(PeerData.Auth_List, null, ipfs.id().get("ID").toString(), (short) 2);
                    if (reply != null) {
                        AuxilaryIpfs.send(PeerId, reply);
                    } else {
                        PeerData.dlog.log("Nulled msg");
                    }
                }
            }
        }
        bytes_array = null;
        rbuff = null;
        decodedString = null;

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

// This class contains the main IPLS methods. Those methods are:
// * Init() : Calling Init each peer joins the IPLS network, initializes the data structures and becomes responsible
//      for some partitions
// * UpdateModel(List<Double> Gradients) : Takes as input the gradients vector, partitions the vector to the
//      corresponding partitions, and sends the partitions to the selected aggregators. In case the peer is an
//      aggregator, aggregates the partitions he is responsible for, synchronizes his partitions, and publishes
//      his updated partitions. Then each peer receives and caches the updated partitions, and leaves the UpdateModel.
// * GetPartitions() : Returns the updated model

public class IPLS {
    String my_id;
    static IPFS ipfs;
    static MyIPFSClass ipfsClass;
    static IPLS_Comm commit;
    static int _PARTITIONS = PeerData._PARTITIONS;
    static int _MIN_PARTITIONS = PeerData._MIN_PARTITIONS;
    public static int num = 0;
    SwarmManager SwarmManagerThread;
    ThreadReceiver _New_Peer;
    ThreadReceiver _Personal_Thread;
    ThreadReceiver _Auth_Listener;
    Updater UpdaterThread;
    String FileName;
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_RESET = "\u001B[0m";


    public IPLS(String path,String fileName,List<String> BootstrampPeers,boolean bootstraper, long model_size){
        PeerData.Bootstrapers = BootstrampPeers;
        PeerData.Path = path;
        PeerData._MODEL_SIZE = model_size;
        PeerData.isBootsraper = bootstraper;
        this.FileName = fileName;

    }

    // This method checks if there are partitions that no one found
    //  to be responsible for. Note that in reality there exist some
    //  peers responsible for that partition, but due to network issues
    //  the peer was unable to communicate with those peers. On the end
    //  it returns a list of those partitions if such list exists.
    public List<Integer> Find_Orphan_Partitions(){
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


    // Using this method a peer determines the partitions that he will be responsible for
    // First the peer will search for the most overloaded peer (i.e the peer who is responsible
    // for the most partitions which are bigger than the minimum amount of partitions that a peer
    // can be responsible for). If an overloaded peer exists, then the peer chooses min partitions
    // from the partitions he is responsible for. Otherwise the peer selects the least replicated
    // partitions. Upon selecting the peers that the peer should be responsible for, publishes to
    // the network his responsibilities. In case he choosed an overloaded peer, then upon receiving
    // this message, if he still contains some of those partitions, and he is still overloaded, then
    // he deletes his some of those responsibilities until he reaches min partitions.
    public void select_partition() throws Exception {
        int i,new_key,rand,max_loaded= 0;
        List<Integer> Peer_Auth = new ArrayList<Integer>();
        List<Peer> Peers = ipfs.swarm.peers();

        Map<Integer,Integer> Partition_Cardinality = new HashMap<Integer, Integer>();
        String Loaded_Peer = null;
        Random Randint = new Random();

        // Check if there an overloaded peer in the swarm
        for(i = 0; i < Peers.size(); i++){
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
                PeerData.Clients.put(Peer_Auth.get((i + rand)%Peer_Auth.size()),new ArrayList<>());
            }
            PeerData.dlog.log(PeerData.Auth_List);
            // Publish the auth_List to the network and let the Loaded peer now that you took some of his partitions
            // in order to be able to delete them
            ipfsClass.send("Authorities",ipfsClass.Marshall_Packet(PeerData.Auth_List,Loaded_Peer,ipfs.id().get("ID").toString(),(short)1));

            return;
        }

        //Else select least frequently used partition
        for(i = 0; i < _PARTITIONS; i++){
            Partition_Cardinality.put(i,PeerData.Partition_Availability.get(i).size());
        }

        for(i = 0; i < _MIN_PARTITIONS; i++){
            new_key = Collections.min(Partition_Cardinality.entrySet(),Comparator.comparing(Map.Entry::getValue)).getKey();
            PeerData.Auth_List.add(new_key);
            PeerData.Clients.put(new_key,new ArrayList<>());
            Partition_Cardinality.remove(new_key);
        }
        // Publish the auth_list to the network.
        ipfsClass.send("Authorities",ipfsClass.Marshall_Packet(PeerData.Auth_List,null,ipfs.id().get("ID").toString(),(short) 2));

    }



    //Partition Gradient vector into a set of smaller vectors corespoding to a partition ID
    public Map<Integer,double[]> OrganizeGradients(List<Double> Gradients){
        int i,j,chunk_size = (int)(PeerData._MODEL_SIZE/PeerData._PARTITIONS) +1;
        Map<Integer,double[]> Partition = new HashMap<Integer, double[]>();

        for(i = 0; i < PeerData._PARTITIONS; i++){
            double[] Gradient_Partition;
            if((i+1)*chunk_size > PeerData._MODEL_SIZE){
                Gradient_Partition = new double[(int)PeerData._MODEL_SIZE - i*chunk_size  +1];
            }
            else{
                Gradient_Partition = new double[chunk_size + 1];
            }
            for(j = i*chunk_size; j < (i+1)*chunk_size && j < Gradients.size(); j++){
                Gradient_Partition[j-i*chunk_size] = Gradients.get(j);
            }

            Gradient_Partition[j-i*chunk_size] = 1;

            Partition.put(i,Gradient_Partition);
        }
        return Partition;

    }

    // This method is called by a peer in order to select aggregators for partitions
    // he is not responsible for
    public void select_aggregators() throws Exception{
        PeerData.mtx.acquire();
        // Select the aggregators to send the gradient partitions in the near future
        // Note that aggregators == Dealers
        for(int i = 0; i < PeerData._PARTITIONS ; i++) {
            if(!PeerData.Auth_List.contains(new Integer(i))){
                if (PeerData.Partition_Availability.get(i).size() == 0) {
                    // handle Crash
                }
                else {
                    if (!PeerData.Dealers.containsKey(i)) {
                        //Select an arbitary peer for now to become the partitions server
                        int size = PeerData.Partition_Availability.get(i).size();
                        Random rn = new Random();
                        int pos = Math.abs(rn.nextInt() % size);
                        PeerData.Dealers.put(i, PeerData.Partition_Availability.get(i).get(pos));
                    }
                    if(PeerData.isSynchronous){
                        //Inform aggregator that you are going to send to him the gradients for the partition i in
                        // the future iterations. Also ask him to give his latest updated partition for the partition i
                        ipfsClass.send(PeerData.Dealers.get(i),ipfsClass.JOIN_PARTITION_SERVER(PeerData._ID,i,(short)0));
                    }
                }
            }
        }

        PeerData.mtx.release();

    }


    // This is one of the main methods called by the IPLS instance. This method reconstructs the updated
    // model from the cached partitions contained in the PeerData.Weights. There only special case about
    // this method is when a peer enters the IPLS network, where he needs 1) to select the aggregators
    // that he is going to send his gradient partitions in the future 2) retrieve the latest updated
    // partitions before start training the model.
    public List<Double> GetPartitions() throws Exception {
        int i,j;
        List<Double> Parameters = new ArrayList<>();

        // Retrieve the latest model in case this is your first iteration
        if(PeerData.First_Iter){
            // This is in the case of purely asynchronous SGD which is rarely used
            if(!PeerData.isSynchronous){
                for(i = 0; i < PeerData._PARTITIONS; i++){
                    for(j = 0; j < PeerData.Weights.get(i).length; j++){
                        Parameters.add(PeerData.Weights.get(i)[j]);
                    }
                }
                PeerData.First_Iter = false;
                // Select the aggregators for the partitions you are not responsible for
                select_aggregators();
                return  Parameters;
            }
            if(PeerData.isSynchronous){
                // Wait to receive all updated partitions
                PeerData.dlog.log(">>>>> GET PARTITIONS : " + PeerData.Wait_Ack);
                while(PeerData.Wait_Ack.size() != 0 && (ipfsClass.training_elapse_time(ipfsClass.find_iter()) - ipfsClass.get_training_time()/6) > ipfsClass.get_curr_time()){Thread.yield();}
                // Select the aggregators for the partitions you are not responsible for
                select_aggregators();
                PeerData.mtx.acquire();
                // This condition holds only for strictly synchronous SGD which is not so practical
                if(PeerData.First_Iter && !PeerData.Relaxed_SGD ){
                    // Synchronize with the slowest
                    int min_iteration = 100000000;
                    int max_iteration = 0;
                    boolean zero = false;
                    for(i = 0; i < _PARTITIONS; i++){
                        if(!PeerData.Auth_List.contains(i) && PeerData.Servers_Iteration.containsKey(PeerData.Dealers.get(i)) && PeerData.Servers_Iteration.get(PeerData.Dealers.get(i)) == 0){
                            zero = true;
                        }
                        if(!PeerData.Auth_List.contains(i) && PeerData.Servers_Iteration.containsKey(PeerData.Dealers.get(i)) && PeerData.Servers_Iteration.get(PeerData.Dealers.get(i)) < min_iteration && PeerData.Servers_Iteration.get(PeerData.Dealers.get(i)) != 0){
                            min_iteration = PeerData.Servers_Iteration.get(PeerData.Dealers.get(i));
                        }
                        if(!PeerData.Auth_List.contains(i) && PeerData.Servers_Iteration.containsKey(PeerData.Dealers.get(i)) && PeerData.Servers_Iteration.get(PeerData.Dealers.get(i)) > max_iteration ){
                            max_iteration = PeerData.Servers_Iteration.get(PeerData.Dealers.get(i));
                        }
                    }
                    if(min_iteration == 100000000){
                        min_iteration = 0;
                    }
                    if(zero == true && max_iteration ==1){
                        min_iteration = 0;
                    }
                    PeerData.middleware_iteration = min_iteration;
                    PeerData.dlog.log("CLOCK SYNCHRONIZED IN : " + PeerData.middleware_iteration);
                }
                for(i = 0; i < PeerData.Auth_List.size(); i++){
                    ipfsClass.send(String.valueOf(PeerData.Auth_List.get(i)),ipfsClass.JOIN_PARTITION_SERVER(PeerData._ID,PeerData.Auth_List.get(i),(short)0));
                }

                PeerData.mtx.release();
            }
       }

        // Check if Updated Partition already does not exist locally
        for(i = 0; i < PeerData._PARTITIONS; i++){
            PeerData.Weights.put(i,PeerData.Weight_Address.get(i));
        }
        if(PeerData.First_Iter){
            PeerData.dlog.log("UPDATES DOWNLOADED : " + PeerData.downloaded_updates + "/" + new Integer(PeerData._PARTITIONS ));
            PeerData.downloaded_updates = 0;
            //Wait for some time to collect hash_partitions
            //Thread.sleep(2000);
            // Get the data of the partitions you are responsible for from the peer that was responsible for those partitions
            //for(i = 0; i < PeerData.Auth_List.size(); i++){
            //    if(PeerData.Hash_Partitions.containsKey(PeerData.Auth_List.get(i)) && PeerData.First_Iter){
            //        PeerData.Weights.put(PeerData.Auth_List.get(i),
            //                (List<Double>) ipfsClass.DownloadParameters(PeerData.Hash_Partitions.get(PeerData.Auth_List.get(i))));
            //    }
            //}
        }



        // Create a parameter vector
        for(i = 0; i < PeerData._PARTITIONS; i++){
            for(j = 0; j < PeerData.Weights.get(i).length - 1; j++){
                if(PeerData.Weights.get(i)[PeerData.Weights.get(i).length-1] == 0.0){
                    Parameters.add(PeerData.Weights.get(i)[j]);
                }
                else{
                    if(PeerData.secure_ipls){
                        Parameters.add(PeerData.Weights.get(i)[j]/(Math.pow(10,12)*PeerData.Weights.get(i)[PeerData.Weights.get(i).length-1]));
                    }
                    else{
                        Parameters.add(PeerData.Weights.get(i)[j]/PeerData.Weights.get(i)[PeerData.Weights.get(i).length-1]);
                    }
                }
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

        PeerData.dlog.log(Peer);
        if(Peer == null){
            PeerData.dlog.log("An error occurred on loading the model. Exiting...");
        }
        while (true) {
            //Get first peer in list

            org.javatuples.Pair<String,Integer> pair = new org.javatuples.Pair(Peer, 0);

            //PeerData.Wait_Ack.add(pair);
            ipfsClass.send(Peer, ipfsClass.Marshall_Packet(ipfs.id().get("ID").toString(), (short) 5));
            //Wait until received the request
            while (PeerData.Wait_Ack.size() != 0) { Thread.yield();}
            if(PeerData.Weights.get(0).length != 0){
                break;
            }
        }
    }

    // This method when called, further aggregates replicas collected from
    // aggregators that are unavailable in the synchronization phase. In
    // the aggregation phase when an aggregator finishes downloading his own
    // gradients starts downloading other replicas aggregators partition gradients
    // and if a replicator doesn't send him in the synchronization phase, then
    // uses as replica message the gradients he already downloaded.
    public void Collect_Replicas(){
        List<Pair<Integer,String>> keys = new ArrayList<>(PeerData.Other_Replica_Gradients.keySet());
        Double gradient_value;
        int partition;
        // For each unavailable replica aggregator aggregate his locally downloaded gradients
        // to the Replocas_Gradients giving the impression the replica aggragator was available
        for(int i = 0; i < keys.size(); i++){
            partition = keys.get(i).getValue0();
            for(int j = 0; j < PeerData.Other_Replica_Gradients.get(keys.get(i)).length; j++){
                gradient_value = PeerData.Replicas_Gradients.get(partition)[j];
                PeerData.Replicas_Gradients.get(partition)[j]  = gradient_value + PeerData.Other_Replica_Gradients.get(keys.get(i))[j];

                if(!PeerData.Participants.containsKey(partition)){
                    PeerData.Participants.put(partition,PeerData.Other_Replica_Gradients_Received.get(keys.get(i)));
                }
                else{
                    PeerData.Participants.replace(partition,PeerData.Participants.get(partition) + PeerData.Other_Replica_Gradients_Received.get(keys.get(i)));
                }
            }
        }
        // Clear data structures to prepare for the next IPLS round
        PeerData.Other_Replica_Gradients = new HashMap<>();
        PeerData.Other_Replica_Gradients_Received = new HashMap<>();
        keys = null;
    }


    // In this method the local aggregated partition is aggregated by the aggregated partitions
    // provided by all other aggregators responsible for the same partition. Upon sumarization
    // the fully aggregated partition is divided by the number of partitcipants that managed to
    // upload gradients for this partition.
    public void AggregatePartition(int Partition) throws InterruptedException {
        if(!PeerData.Participants.containsKey(Partition)){
            PeerData.Participants.put(Partition,PeerData.workers.get(Partition).size()+1);
        }
        else{
            PeerData.Participants.replace(Partition,PeerData.workers.get(Partition).size()  +  PeerData.Participants.get(Partition));
        }
        for(int i = 0; i < PeerData.Weights.get(Partition).length; i++) {
            PeerData.Weights.get(Partition)[i] = (PeerData.Aggregated_Gradients.get(Partition)[i] + PeerData.Replicas_Gradients.get(Partition)[i]);
        }
        System.out.println("Cardinarity : " + PeerData.Weights.get(Partition)[PeerData.Weights.get(Partition).length-1]);
        for(int i = 0; i < PeerData.Weight_Address.get(Partition).length; i++){
            if(PeerData.secure_ipls){

            }
            else{
                PeerData.Weight_Address.get(Partition)[i] = PeerData.Weights.get(Partition)[i];
            }
            // Upon aggregation initialize the aggregated gradients and the replicas gradients so that
            // it can be used in the next IPLS round
            PeerData.Aggregated_Gradients.get(Partition)[i] = 0.0;
            PeerData.Replicas_Gradients.get(Partition)[i] = 0.0;
        }
        PeerData.workers.put(Partition,new ArrayList<>());

        PeerData.Participants.replace(Partition,1);
    }

    // This method is used by participants who want to change the aggregator
    // they send the gradients partition for the given partition.
    public void Select_New_Dealer(int Partition) throws Exception {
        int size = PeerData.Partition_Availability.get(Partition).size();
        Random rn = new Random();
        int pos = Math.abs(rn.nextInt()%size);
        PeerData.Dealers.put(Partition,PeerData.Partition_Availability.get(Partition).get(pos));
        if(PeerData.isSynchronous) {
            //Send join request
            PeerData.dlog.log(">>>CHOOSING NEW SERVER ");
            ipfsClass.send(PeerData.Dealers.get(Partition), ipfsClass.JOIN_PARTITION_SERVER(PeerData._ID, Partition, (short) 0));
        }
    }

    public void Send_Gradient_Partition(int Partition, Map<Integer,double[]> Gradient) throws Exception {
        String Peer = PeerData.Dealers.get(Partition);
        if(PeerData.isSynchronous ) {
            PeerData.Wait_Ack.add( new Triplet<>(Peer,Partition,PeerData.middleware_iteration));
            // Send the updates
            if(!PeerData.IPNS_Enable){
                if(Gradient != null){
                    if(PeerData.Indirect_Communication){
                        commit.commit_partition_update(Peer,Gradient.get(Partition),Partition,(short)23);
                        //System.gc();
                    }
                    else{
                        ipfsClass.send(Peer,ipfsClass.Marshall_Packet(Gradient.get(Partition),ipfs.id().get("ID").toString(),Partition,PeerData.middleware_iteration,(short) 3));
                        System.gc();
                    }
                }
                else{
                    if(PeerData.Indirect_Communication){
                        commit.commit_partition_update(Peer,null,Partition,(short) 23);
                    }
                    else{
                        ipfsClass.send(Peer,ipfsClass.Marshall_Packet((List<Double>) null,ipfs.id().get("ID").toString(),Partition,PeerData.middleware_iteration,(short) 3));
                    }
                }
            }
            //SEND GRADIENTS
            PeerData.dlog.log("SEND GRADIENTS : " + Partition + " , " + Peer);
        }
        else{
            PeerData.Wait_Ack.add( new Triplet<>(Peer,Partition,PeerData.middleware_iteration));
            ipfsClass.send(Peer,ipfsClass.Marshall_Packet(Gradient.get(Partition),ipfs.id().get("ID").toString(),Partition,PeerData.middleware_iteration,(short) 3));
        }
    }

    public void Send_keys() throws Exception{
        String Peer;
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            if(PeerData.key_dir.containsKey(i)){
                Peer = PeerData.Dealers.get(i);
                commit.send_key(Peer,PeerData.key_dir.get(i).getValue0(),i,PeerData.key_dir.get(i).getValue1());
            }
        }
    }

    // Check if there is any peer ahead of your time that has send you
    // the updates already in order not to wait for him for no reason
    public void Update_WaitAck_List(){
        List<Triplet<String,Integer,Integer>> Black_List = new ArrayList<>();
        for(int i = 0;PeerData.isSynchronous &&  PeerData.Wait_Ack_from_future !=null && i < PeerData.Wait_Ack_from_future.size() ; i++){
            if(PeerData.Wait_Ack_from_future.get(i).getValue2() == PeerData.middleware_iteration){
                PeerData.Wait_Ack.remove(PeerData.Wait_Ack_from_future.get(i));
                Black_List.add(PeerData.Wait_Ack_from_future.get(i));
            }
        }
        for(int i = 0; i < Black_List.size(); i++){
            PeerData.Wait_Ack_from_future.remove(Black_List.get(i));
        }
        Black_List = null;
    }

    public void SendGradients(List<Integer> Partitions,Map<Integer,double[]> GradientPartitions) throws Exception {
        long _Start_time = 0;
        PeerData.SendMtx.acquire();
       // PeerData.mtx.acquire();
        _Start_time = System.currentTimeMillis();
        for(int i = 0; i < Partitions.size(); i++){
            //In case where there is no peer available do something
            if(PeerData.Partition_Availability.get(Partitions.get(i)).size() != 0){
                // There is a case where a peer discarded a responsibility so you must search for another peer
                // to join
                if(!PeerData.Dealers.containsKey(Partitions.get(i))){
                    Select_New_Dealer(Partitions.get(i));
                }
                // If everything is ok and you still remain in training phase update for that partition
                if(ipfsClass.get_curr_time() < ipfsClass.training_elapse_time(PeerData.middleware_iteration) ){
                    // Get the peer who is responsible for the partition
                    if(GradientPartitions != null){
                        Send_Gradient_Partition(Partitions.get(i),GradientPartitions);
                    }
                    else{
                        Send_Gradient_Partition(Partitions.get(i),null);
                    }
                }
            }
            else{
                PeerData.dlog.log(">>>> NO PEER RESPONSIBLE FOR THAT PARTITION ( " + Partitions.get(i) +  " ) FOUND :(");
            }
        }
        if(PeerData.IPNS_Enable){
            ipfsClass.publish_gradients(PeerData.GradientPartitions,2);
        }
        System.out.println("SENDED DATA " +  new Long(System.currentTimeMillis() - _Start_time));
        // After saving the gradients file then publish their hashes.
        if(PeerData.Indirect_Communication){
            commit.publish_commitments((short)23);
        }
        PeerData.dlog.log("GRADIENTS STORE TIME : " + new Long(System.currentTimeMillis() - _Start_time));
        System.out.println("Gradients Upload time : " + new Long(System.currentTimeMillis() - _Start_time));
        PeerData.upload_time.add(System.currentTimeMillis() - _Start_time);
        Update_WaitAck_List();
        //This variable indicates that you have sent the updated values. There is a case where
        // the peer who sent you the updates, discarded its responsibility or left the network
        // so knowing that you have "wasted" your updates is crucial so that you wish to resend
        // them to the new server or just avoid waiting for the updated model.
        PeerData.sendingGradients = true;
        //PeerData.mtx.release();

        PeerData.SendMtx.release();


    }

    public void Wait_Client_Gradients() throws Exception {
        int i,j;
        String Peer,Hash;
        Map<Integer,String> update_hash = new HashMap<>();

        //Wait to receive all gradients from clients

        while(PeerData.isSynchronous && PeerData.Client_Wait_Ack.size() != 0 ){Thread.yield();}
        List<Integer> Temp_Auth_List = new ArrayList<>();

        for(i = 0; i < PeerData.Auth_List.size(); i++){
            Temp_Auth_List.add(PeerData.Auth_List.get(i));
        }
        System.out.println(ANSI_YELLOW+"GRADIENTS DOWNLOADED : " + PeerData.commited_hashes + " ,Gradients downloaded :" + PeerData.downloaded_hashes + " time: " + new Long(System.currentTimeMillis() - PeerData.Start_download_time) + ANSI_RESET);
        //Next the peer must aggregate his gradients with the other peers
        //while(ipfsClass.get_curr_time() < ipfsClass.synch_elapse_time(PeerData.middleware_iteration)){}
        if(PeerData.pure_aggregation_time.size() != PeerData.middleware_iteration + 1){
            PeerData.pure_aggregation_time.add(System.currentTimeMillis() - PeerData.Start_download_time);
        }
        PeerData.aggregation_time.add(System.currentTimeMillis() - PeerData.Start_download_time);

        for(i = 0; i < Temp_Auth_List.size() && !PeerData.IPNS_Enable; i++){
            if(PeerData.Indirect_Communication){
                Hash = commit.commit_partial_update(PeerData.Aggregated_Gradients.get(Temp_Auth_List.get(i)), Temp_Auth_List.get(i), PeerData.workers.get(PeerData.Auth_List.get(i)).size()+1 );
                update_hash.put(Temp_Auth_List.get(i),Hash);
            }
            else{
                ipfsClass.send(new Integer(Temp_Auth_List.get(i)).toString(),ipfsClass.Marshall_Packet(PeerData.Aggregated_Gradients.get(PeerData.Auth_List.get(i)),ipfs.id().get("ID").toString(),PeerData.middleware_iteration,PeerData.workers.get(PeerData.Auth_List.get(i)).size()+1,(short) 3));
            }
        }
        System.out.println("Partial Updates Sent");
        long Pu = System.currentTimeMillis();
        if(PeerData.Indirect_Communication){
            commit.write_partial_updates(update_hash);
        }
        System.out.println(Pu - System.currentTimeMillis());
        update_hash=  new HashMap<>();
        if(PeerData.IPNS_Enable){
            ipfsClass.publish_gradients(PeerData.GradientPartitions,3);
        }
        System.out.println(PeerData.Replica_Wait_Ack.size() + " , " + PeerData.Replica_Wait_Ack);
        while(ipfsClass.get_curr_time() < ipfsClass.synch_elapse_time(PeerData.middleware_iteration)  && PeerData.Replica_Wait_Ack.size() != 0 ){Thread.yield();}
        //sendall the updated data
        PeerData.dlog.log("ALL REPLICAS SEND GRADIENTS :^) " + PeerData.Replica_holders);

        PeerData.Received_Replicas = new ArrayList<>();
        PeerData.dlog.log("NEW CLIENTS " + PeerData.New_Clients);
        Collect_Replicas();
        PeerData.updates_hashes = new ArrayList<>();

        PeerData.round_time.add(System.currentTimeMillis() - (long)(ipfsClass.training_elapse_time(PeerData.middleware_iteration))*1000 + ipfsClass.get_training_time()*1000);
        PeerData.iter_time.add(System.currentTimeMillis() - PeerData.Start_download_time);
        if(!PeerData.Indirect_Communication){
            PeerData.Start_download_time= 0;
        }
        System.out.println("Round time : "  + PeerData.round_time.get(PeerData.round_time.size()-1) + " , " + PeerData.iter_time.get(PeerData.iter_time.size()-1));
        if(PeerData.premature_termination == false || PeerData.flush == false){

            int _Start_time = ipfsClass.get_curr_time();
            // Before do anything upload the updated partition in IPFS system
            for(i = 0; i < Temp_Auth_List.size(); i++){
                // aggregate the gradients from all the unavailable aggregators
                AggregatePartition(Temp_Auth_List.get(i));
                Hash  = commit.commit_update(PeerData.Weights.get(Temp_Auth_List.get(i)),Temp_Auth_List.get(i));
                update_hash.put(Temp_Auth_List.get(i),Hash);
                PeerData.updates_hashes.add(Hash);
            }
            PeerData.dlog.log("Store time of Updates : " + new Integer(ipfsClass.get_curr_time() - _Start_time));
            commit.publish_updates(update_hash);
            // Wait until synchronization round is finished so that you can proceed in the next round
        }
        if(!PeerData.premature_termination){
            while(ipfsClass.get_curr_time() < ipfsClass.synch_elapse_time(PeerData.middleware_iteration)){
                PeerData.dlog.log("Waiting for the iteration to finish, current time :  " + ipfsClass.get_curr_time() + " time until finishing : " + ipfsClass.synch_elapse_time(PeerData.middleware_iteration));

                int sleeptime = ipfsClass.synch_elapse_time(PeerData.middleware_iteration) - ipfsClass.get_curr_time();
                if(sleeptime > 0){
                    System.gc();
                    Thread.sleep(1000*sleeptime);}
            }
        }
        else{
            while(PeerData.middleware_iteration == ipfsClass.find_iter()){
                Thread.yield();
            }
            System.out.println(ANSI_YELLOW + " NEW ITERATION BEGINS " + ANSI_RESET);
            PeerData.flush = false;
        }

        // Inform peers about new partitions
        for(i = 0; i < Temp_Auth_List.size() && PeerData.isSynchronous; i++){
            PeerData.mtx.acquire();
            //if(PeerData.Auth_List.contains(Temp_Auth_List.get(i))){
            //    PeerData.mtx.release();
            //    continue;
            //}
            //if(!PeerData.IPNS_Enable){
            //    ipfsClass.send("New_Peer",ipfsClass.Marshall_Packet(update_hash.get(Temp_Auth_List.get(i)),PeerData._ID,Temp_Auth_List.get(i),PeerData.middleware_iteration,(short)24));
            //}


            for(j = 0;PeerData.Clients.get(Temp_Auth_List.get(i))!=null &&  j < PeerData.Clients.get(Temp_Auth_List.get(i)).size(); j++){
                //Get the peer registered and send him the updated partition
                Peer = PeerData.Clients.get(Temp_Auth_List.get(i)).get(j);

                //if(!PeerData.IPNS_Enable){
                    //ipfsClass.send(Peer,ipfsClass.Marshall_Packet(PeerData.Weights.get(PeerData.Auth_List.get(i)),ipfs.id().get("ID").toString(),PeerData.Auth_List.get(i),PeerData.middleware_iteration,(short)4));
                //    ipfsClass.send(Peer,ipfsClass.Marshall_Packet(update_hash.get(PeerData.Auth_List.get(i)),PeerData._ID,PeerData.Auth_List.get(i),PeerData.middleware_iteration,(short)24));
                //}


                PeerData.Client_Wait_Ack.add(new Triplet<>(Peer,Temp_Auth_List.get(i),PeerData.middleware_iteration+1));
            }
            PeerData.mtx.release();

            //for(j = 0; j < PeerData.Replica_holders.get(PeerData.Auth_List.get(i)).size(); j++){
            //    Peer = PeerData.Replica_holders.get(PeerData.Auth_List.get(i)).get(j);
            //    PeerData.Replica_Wait_Ack.add(new Pair<>(Peer,PeerData.Auth_List.get(i)));
            //}

        }

        PeerData.Data_download.add(PeerData.data_received);
        PeerData.data_received = 0;


    }

    public void Update_Client_WaitAck_List() throws Exception{
        List<Triplet<String,Integer,Integer>> Black_List = new ArrayList<>();

        for(int i = 0;PeerData.Client_Wait_Ack_from_future != null && i < PeerData.Client_Wait_Ack_from_future.size(); i++){
            if(PeerData.Client_Wait_Ack_from_future.get(i).getValue2() > PeerData.middleware_iteration + 1){
                PeerData.Client_Wait_Ack.remove(new Triplet<>(PeerData.Client_Wait_Ack_from_future.get(i).getValue0(),PeerData.Client_Wait_Ack_from_future.get(i).getValue1(),PeerData.middleware_iteration + 1));
            }
            else if(PeerData.Client_Wait_Ack_from_future.get(i).getValue2() == PeerData.middleware_iteration + 1){
                PeerData.Client_Wait_Ack.remove(PeerData.Client_Wait_Ack_from_future.get(i));
                Black_List.add(PeerData.Client_Wait_Ack_from_future.get(i));
            }
            else if(PeerData.Client_Wait_Ack_from_future.get(i).getValue2() == PeerData.middleware_iteration){
                // This was in older version
                //ipfsClass.send(PeerData.Client_Wait_Ack_from_future.get(i).getValue0(),ipfsClass.Marshall_Packet(PeerData.Weights.get(PeerData.Client_Wait_Ack_from_future.get(i).getValue1()),ipfs.id().get("ID").toString(),PeerData.Client_Wait_Ack_from_future.get(i).getValue1(),PeerData.middleware_iteration,(short)4));

                Black_List.add(PeerData.Client_Wait_Ack_from_future.get(i));

            }
            else{
                PeerData.dlog.log("!!! WARNING !!! THE UNTHINKABLE HAPPENED");
            }
        }
        for(int i = 0; i < Black_List.size(); i++){
            PeerData.Client_Wait_Ack_from_future.remove(Black_List.get(i));
        }

        // Replace the Aggregated Gradients with those collected from future, and the gradients from future to zero
        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            for(int j = 0; j < PeerData.Aggregated_Gradients.get(PeerData.Auth_List.get(i)).length; j++){
                PeerData.Aggregated_Gradients.get(PeerData.Auth_List.get(i))[j] = PeerData.Aggregated_Gradients_from_future.get(PeerData.Auth_List.get(i)).get(j);
                PeerData.Aggregated_Gradients_from_future.get(PeerData.Auth_List.get(i)).set(j,0.0);
            }
        }
        Black_List = null;
    }

    public void Update_replicas_structures(){
        List<Triplet<String,Integer,Integer>> blacklist = new ArrayList<>();
        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            for(int j = 0; j < PeerData.New_Replicas.get(PeerData.Auth_List.get(i)).size(); j++){
                PeerData.Replica_holders.get(PeerData.Auth_List.get(i)).add(PeerData.New_Replicas.get(PeerData.Auth_List.get(i)).get(j));
            }
            PeerData.New_Replicas.put(PeerData.Auth_List.get(i),new ArrayList<>());
        }
        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            for(int j = 0; j < PeerData.Replica_holders.get(PeerData.Auth_List.get(i)).size(); j++) {
                if (!PeerData.Replica_Wait_Ack.contains(new Triplet<>(PeerData.Replica_holders.get(PeerData.Auth_List.get(i)).get(j), PeerData.Auth_List.get(i), PeerData.middleware_iteration + 1))) {
                    PeerData.Replica_Wait_Ack.add(new Triplet<>(PeerData.Replica_holders.get(PeerData.Auth_List.get(i)).get(j), PeerData.Auth_List.get(i), PeerData.middleware_iteration + 1));
                }
            }
        }
        for(int i = 0; i < PeerData.Replica_Wait_Ack_from_future.size(); i++){
            if(PeerData.Replica_Wait_Ack.contains(PeerData.Replica_Wait_Ack_from_future.get(i))){
                blacklist.add(PeerData.Replica_Wait_Ack_from_future.get(i));
                PeerData.Replica_Wait_Ack.remove(PeerData.Replica_Wait_Ack_from_future.get(i));
            }
            else if(PeerData.Replica_Wait_Ack.contains(new Triplet<>(PeerData.Replica_Wait_Ack_from_future.get(i).getValue0(),PeerData.Replica_Wait_Ack_from_future.get(i).getValue1(),PeerData.middleware_iteration+1))){
                if(PeerData.middleware_iteration + 1< PeerData.Replica_Wait_Ack_from_future.get(i).getValue2()){
                    PeerData.Replica_Wait_Ack.remove(new Triplet<>(PeerData.Replica_Wait_Ack_from_future.get(i).getValue0(),PeerData.Replica_Wait_Ack_from_future.get(i).getValue1(),PeerData.middleware_iteration+1));
                }
            }
        }
        for(int i = 0; i < blacklist.size(); i++){
            PeerData.Replica_Wait_Ack_from_future.remove(blacklist.get(i));
        }
        blacklist = null;
    }
    // In case where there were clients wanted to join in the middle of the iteration add them officially
    // to the clients list
    public void Update_Client_List() throws Exception {
        int i,j;
        String Peer;
        PeerData.mtx.acquire();

        for(i = 0; i < PeerData.Auth_List.size(); i++){

            for(j = 0; j < PeerData.New_Clients.get(PeerData.Auth_List.get(i)).size(); j++ ){
                Peer = PeerData.New_Clients.get(PeerData.Auth_List.get(i)).get(j);
                PeerData.Clients.get(PeerData.Auth_List.get(i)).add(Peer);
                // this was in older version not needed now
                // ipfsClass.send(Peer,ipfsClass.Marshall_Packet(PeerData.Weights.get(PeerData.Auth_List.get(i)),ipfs.id().get("ID").toString(),PeerData.Auth_List.get(i),PeerData.middleware_iteration,(short)4));
                PeerData.Client_Wait_Ack.add(new Triplet<>(Peer,PeerData.Auth_List.get(i),PeerData.middleware_iteration+1));
            }
            PeerData.New_Members.put(PeerData.Auth_List.get(i),new ArrayList<>());
            PeerData.New_Clients.put(PeerData.Auth_List.get(i),new ArrayList<>());
        }

        Update_Client_WaitAck_List();
        Update_replicas_structures();

        if(!PeerData.Relaxed_SGD) {
            if (PeerData.First_Iter) {
                PeerData.First_Iter = false;
                PeerData.middleware_iteration++;
            } else {
                PeerData.middleware_iteration++;
            }
        }

        //PeerData.Client_Wait_Ack_from_future = new ArrayList<>();
        PeerData.sendingGradients = false;
        PeerData.mtx.release();

        if(PeerData.Relaxed_SGD){
            int iter = ipfsClass.find_iter();
            while(iter == -1){
                Thread.yield();
                iter = ipfsClass.find_iter();
            }
            if(PeerData.middleware_iteration +1 != iter){
                PeerData.dlog.log("PeerData.middleware_iteration +1 != iter , middleware iteration : " + PeerData.middleware_iteration + " , iter : " + iter );
            }
            if (PeerData.First_Iter) {
                PeerData.First_Iter = false;
            }
            PeerData.mtx.acquire();
            PeerData.dlog.log(PeerData.middleware_iteration);
            PeerData.middleware_iteration = iter;
            PeerData.mtx.release();
        }


    }

    public void retrieve_updates(int iter) throws Exception{
        // Hash, partition, Origin
        List<Integer> committed_updates = new ArrayList<>();
        while(ipfsClass.find_iter() != iter + 1){Thread.sleep(100);Thread.yield();}



        List<Triplet<String,Integer,String>> updates = PeerData.ds_client.getUpdates(iter+1);
        List<Quartet<String,String, Integer,Integer>> Task = new ArrayList<>();
        //System.out.println("GOT UPDATES : " + updates);
        //Receive commit <Partition,Iteration,Hash, origin_peer>
        //Quartet<Integer,Integer,String,String> Reply = AuxilaryIpfs.Get_Commitment(rbuff,bytes_array);
        for(int i = 0; i < updates.size(); i++){
            Task.add(new Quartet<>(updates.get(i).getValue0(),
                    updates.get(i).getValue2(),
                    iter,
                    updates.get(i).getValue1()));
            PeerData.dlog.log(ANSI_YELLOW + "Updates : " + updates.get(i).getValue0() + " , " +updates.get(i).getValue1()+ " , " + updates.get(i).getValue2() + ANSI_RESET);
            committed_updates.add(updates.get(i).getValue1());
        }
        // Remove all partitions from wait_Ack list that don't have updates.
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            if(!committed_updates.contains(i)){
                if(PeerData.Dealers.containsKey(i) && PeerData.Wait_Ack.contains(new Triplet<>(PeerData.Dealers.get(i),i,iter))){
                    PeerData.Wait_Ack.remove(new Triplet<>(PeerData.Dealers.get(i),i,iter));
                }
                else if(PeerData.Wait_Ack.contains(new Triplet<>("New_Peer",i,iter))){
                    PeerData.Wait_Ack.remove(new Triplet<>("New_Peer",i,iter));
                }
                else{
                    // In case none of the above triplets found, just remove the one that belongs to the partition i.
                    Triplet remove_triplet = new Triplet<>("",0,0);
                    for(int j = 0; j < PeerData.Wait_Ack.size(); j++){
                        if(PeerData.Wait_Ack.get(j).getValue1() == i){
                            remove_triplet = PeerData.Wait_Ack.get(j);
                            break;
                        }
                    }
                    PeerData.Wait_Ack.remove(remove_triplet);
                }
            }
        }
        PeerData.updates_download_scheduler.add_batched_updates(Task);

    }


    //Update Gradient is an API method, where after each iteration of the
    // Learning phase we send the updated gradients to other peers
    public void UpdateGradient(List<Double> Gradients) throws Exception {
        int i,j;
        List<Integer> Partitions = new ArrayList<>();
        Map<Integer,double[]> GradientPartitions = new HashMap<>();
        //There is a possibility that Gradients List is going to be null. This might happen when the peer
        // did not trained the model in time thus he has nothing to give.
        if(Gradients != null){
            GradientPartitions = OrganizeGradients(Gradients);
        }
        else{
            GradientPartitions = null;
        }


        for(i = 0; i < _PARTITIONS; i++){
            Partitions.add(i);
        }

        System.out.println(PeerData.Dealers);
        for(i = 0; i < PeerData.Auth_List.size(); i++){
            Partitions.remove(PeerData.Auth_List.get(i));
            //Put the request to the Updater
            //System.out.println("PUT GRADIENTS : " +  PeerData.Auth_List.get(i));
            //PeerData.Client_Wait_Ack.add(new Pair<>(PeerData._ID,PeerData.Auth_List.get(i)));
            PeerData.Dealers.put(PeerData.Auth_List.get(i),PeerData._ID);
            PeerData.Wait_Ack.add(new Triplet<>(PeerData._ID,PeerData.Auth_List.get(i),PeerData.middleware_iteration));
            if(!PeerData.isSynchronous){
                if(Gradients == null){

                    PeerData.queue.add(new Sextet<>(new ArrayList<>(Collections.singleton(ipfs.id().get("ID").toString())),PeerData.Auth_List.get(i),PeerData.middleware_iteration,true,null,null));
                    continue;
                }
                PeerData.queue.add(new Sextet<>(new ArrayList<>(Collections.singleton(ipfs.id().get("ID").toString())),PeerData.Auth_List.get(i),PeerData.middleware_iteration,true,GradientPartitions.get(PeerData.Auth_List.get(i)),null));
            }
            else{
                PeerData.mtx.acquire();
                for(j = 0; j < PeerData.Aggregated_Gradients.get(PeerData.Auth_List.get(i)).length && Gradients != null; j++) {
                    PeerData.Aggregated_Gradients.get(PeerData.Auth_List.get(i))[j] = PeerData.Aggregated_Gradients.get(PeerData.Auth_List.get(i))[j] + GradientPartitions.get(PeerData.Auth_List.get(i))[j];
                }
                PeerData.mtx.release();
            }
        }
        PeerData.mtx.acquire();
        for(i = 0; i < PeerData._PARTITIONS; i++){
            if(!PeerData.Auth_List.contains(i) &&  PeerData.Dealers.containsKey(i) && PeerData.Dealers.get(i).equals(PeerData._ID)){
                PeerData.Dealers.remove(i);
            }
        }
        PeerData.mtx.release();
        PeerData.Test_mtx.acquire();
        PeerData.RecvList.add(PeerData.DataRecv);
        PeerData.DataRecv = 0;
        PeerData.Test_mtx.release();


        int Start = ipfsClass.get_curr_time();
        SendGradients(Partitions,GradientPartitions);


        GradientPartitions.clear();


        int End = ipfsClass.get_curr_time();
        GradientPartitions = null;
        System.out.println("SENDING GRADIENTS COMPLETED " + new Integer(End-Start));
        System.out.println("Waiting from : " + ipfsClass.get_curr_time() + " to : " + ipfsClass.training_elapse_time(PeerData.middleware_iteration) );

        while(ipfsClass.get_curr_time() < ipfsClass.training_elapse_time(PeerData.middleware_iteration)){
            //int diff = ipfsClass.training_elapse_time(PeerData.middleware_iteration) - ipfsClass.get_curr_time();
            //PeerData.dlog.log("Waiting from : " + ipfsClass.get_curr_time() + " to : " + ipfsClass.training_elapse_time(PeerData.middleware_iteration) + " , " + diff + " sec");

            //if(diff>0){
                //System.gc();
            //    Thread.sleep(diff*1000);
            //}
            if(PeerData.Client_Wait_Ack.size() == 0 && Start != 0){
                PeerData.pure_aggregation_time.add(System.currentTimeMillis() - PeerData.Start_download_time);
                //PeerData.aggregation_time.add(ipfsClass.g)
                System.out.println("Aggregation time : " + new Long(System.currentTimeMillis() - PeerData.Start_download_time));
                Start = 0;
            }
            Thread.yield();
        }

        //DELETED : Upon the begin of Aggregation phase, publish the secret keys in order the aggregators to
        // be able to decrypt the downloaded files.
        //if(GradientPartitions != null){
        //    Send_keys();
        //}

        //Gradients.clear();

        if(PeerData.isSynchronous){
            Wait_Client_Gradients();
        }
        System.out.println(ANSI_YELLOW + "AGGREGATION PHASE FINISHED, Gradients Committed : " + PeerData.commited_hashes + " ,Gradients downloaded :" + PeerData.downloaded_hashes + ANSI_RESET);
        PeerData.commited_hashes = PeerData.downloaded_hashes = 0;



        retrieve_updates(PeerData.middleware_iteration);
        Start = ipfsClass.get_curr_time();
        PeerData.dlog.log("Wait ACK : " + PeerData.Wait_Ack.size());
        // Wait to get updated partitions that i am not responsible for
        while(PeerData.Wait_Ack.size() != 0 ){Thread.yield();}
        End = ipfsClass.get_curr_time();
        System.out.println(ANSI_YELLOW+"UPDATES DOWNLOADED : " + PeerData.downloaded_updates + "/" + new Integer(PeerData._PARTITIONS) +" , time : " + new Integer(End-Start)+  ANSI_RESET );
        PeerData.downloaded_updates = 0;
        if(PeerData.isSynchronous){
            Update_Client_List();
            PeerData.dlog.log("ALL SERVERS SEND THE UPDATES : " + PeerData.Servers_Iteration);
        }

        if(!PeerData.isSynchronous){
            PeerData.weightsMtx.acquire();

            for(i = 0; i < _PARTITIONS; i++){
                if(PeerData.workers.get(i).size() > 0){
                    double members = PeerData.previous_iter_active_workers.get(i);
                    members = 0.8*(PeerData.workers.get(i).size()+1) + 0.2*members;
                    PeerData.previous_iter_active_workers.put(i,members);
                    PeerData.workers.put(i,new ArrayList<>());
                }
            }
            PeerData.weightsMtx.release();

        }
        PeerData._Iter_Clock++;

        if(PeerData._Iter_Clock == 5){
            if(PeerData.Auth_List.size() > 0){
                //ipfsClass.Update_file("measure/" + PeerData._ID + "_Aggregation",PeerData.aggregation_time);
                //ipfsClass.Update_file("measure/" + PeerData._ID + "_Pure_Aggregation",PeerData.pure_aggregation_time);
                //ipfsClass.Update_file("measure/" + PeerData._ID + "_Data_Received",PeerData.Data_download);
                Map<String,List<Long>> data = new HashMap<>();
                data.put("aggregation_time",PeerData.aggregation_time);
                data.put("pure_aggregation",PeerData.pure_aggregation_time);
                data.put("data_received",PeerData.Data_download);
                data.put("iter_time",PeerData.iter_time);
                data.put("round_time",PeerData.round_time);
                FileOutputStream fos = new FileOutputStream("measure/" + PeerData._ID + "_Aggregation");
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeObject(data);
                oos.close();
                fos.close();
            }
            else{
                ipfsClass.Update_file("measure/" + PeerData._ID + "_Trainer",PeerData.upload_time);
            }
        }

        Partitions = null;
        System.gc();


    }

    public void InitializeWeights(){
        int i,chunk_size = (int)(PeerData._MODEL_SIZE/PeerData._PARTITIONS) + 1;
        for(i = 0; i < PeerData._PARTITIONS; i++){
            if((i+1)*chunk_size  > PeerData._MODEL_SIZE){
                PeerData.Weight_Address.put(i,new double[(int) (PeerData._MODEL_SIZE - i*chunk_size) + 1]);
                PeerData.Weights.put(i,new double[(int) (PeerData._MODEL_SIZE - i*chunk_size) + 1]);
                PeerData.Aggregated_Gradients.put(i,new double[(int) (PeerData._MODEL_SIZE - i*chunk_size) + 1]);
                PeerData.Stored_Gradients.put(i,new double[(int) (PeerData._MODEL_SIZE - i*chunk_size) + 1]);
                PeerData.Replicas_Gradients.put(i,new double[(int) (PeerData._MODEL_SIZE - i*chunk_size) + 1]);
            }
            else{
                PeerData.Weight_Address.put(i,new double[chunk_size + 1]);
                PeerData.Weights.put(i,new double[chunk_size + 1]);
                PeerData.Aggregated_Gradients.put(i,new double[chunk_size + 1]);
                PeerData.Stored_Gradients.put(i,new double[chunk_size + 1]);
                PeerData.Replicas_Gradients.put(i,new double[chunk_size + 1]);

            }

            PeerData.Aggregated_Gradients_from_future.put(i,new ArrayList<>());
        }
    }

    public void InitializeWeights(List<Double> Model){
        int i,j,chunk_size = (int)(PeerData._MODEL_SIZE/PeerData._PARTITIONS) +1;
        for(i = 0; i < PeerData._PARTITIONS; i++){
            for(j = i*chunk_size; j < (i+1)*chunk_size && j < PeerData._MODEL_SIZE; j++){
                PeerData.Weights.get(i)[j-i*chunk_size] = Model.get(j);
                PeerData.Aggregated_Gradients.get(i)[j - i*chunk_size] = 0.0;
                PeerData.Replicas_Gradients.get(i)[j - i*chunk_size] = 0.0;
                PeerData.Aggregated_Gradients_from_future.get(i).add(0.0);
                PeerData.Stored_Gradients.get(i)[j - i*chunk_size] = 0.0;
                PeerData.Weight_Address.get(i)[j-i*chunk_size] = Model.get(j);
            }
            PeerData.Weight_Address.get(i)[j-i*chunk_size] = 0.0;
            PeerData.Weights.get(i)[j - i*chunk_size] = 0.0;
            PeerData.Aggregated_Gradients.get(i)[j - i*chunk_size] = 0.0;
            PeerData.Replicas_Gradients.get(i)[j - i*chunk_size] = 0.0;
            PeerData.Aggregated_Gradients_from_future.get(i).add(0.0);
            PeerData.Stored_Gradients.get(i)[j - i*chunk_size] = 0.0;
        }
    }


    public Multihash save_model() throws IOException {
        Map<Integer,double[]> Weights = new HashMap<>();

        for(int i =  0; i < PeerData.Auth_List.size(); i++){
            Weights.put(PeerData.Auth_List.get(i),PeerData.Weights.get(PeerData.Auth_List.get(i)));
        }
       return ipfsClass.Upload_File(Weights,PeerData._ID + "saved");
    }


    public double Mean_of_partition_Replication(){
        int sum = 0;
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            sum += PeerData.Partition_Availability.get(i).size();
        }

        return (double)sum/(double)PeerData._PARTITIONS;
    }

    //Authorities == Responsibilities
    public List<Integer> Least_Replicated_Authorities(){
        List<Integer> Auth = new ArrayList<>();
        double mean = Mean_of_partition_Replication();
        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            if(PeerData.Partition_Availability.get(PeerData.Auth_List.get(i)).size() < mean){
                Auth.add(PeerData.Auth_List.get(i));
            }
        }
        return  Auth;
    }

    //Find the peers that you want to hold your responsibilities
    public Map<Integer,String> Find_Candidates(){
        List<String> Candidates = new ArrayList<>();
        List<Integer> Least_Replicated = Least_Replicated_Authorities();
        //Initialize priority queue, of the form (Peer,Auth_Size), and put all known peers
        PriorityQueue<org.javatuples.Pair<String,Integer>> queue = new PriorityQueue<>(new Comparator<org.javatuples.Pair<String, Integer>>() {
            @Override
            public int compare(org.javatuples.Pair<String, Integer> objects, Pair<String, Integer> t1) {
                return objects.getValue1()-t1.getValue1();
            }
        });
        PriorityQueue<org.javatuples.Pair<String,Integer>> helpQueue = queue;

        List<String> Peers = new ArrayList<>(PeerData.Swarm_Peer_Auth.keySet());
        Map<Integer,String> Candidates_Map = new HashMap<>();
        Pair<String,Integer> Peer = null;

        //add the elements into the priority queue
        for(int i = 0; i < Peers.size(); i++){
            if(!PeerData.Bootstrapers.contains(Peers.get(i))) {
            	queue.add(new Pair<>(Peers.get(i),PeerData.Swarm_Peer_Auth.get(Peers.get(i)).size()));
            }
        }

        //For each partition, select the peer that holds the
        // Least used partitions and also, does not hold the
        // partition. (In the future we should enhance the critiria)
        for (int i = 0; i < Least_Replicated.size(); i++) {
            for (int j = 0; j < queue.size(); j++) {
                Peer = helpQueue.remove();
                if (!PeerData.Swarm_Peer_Auth.get(Peer.getValue0()).contains(Least_Replicated.get(i))) {
                    Candidates_Map.put(Least_Replicated.get(i), Peer.getValue0());
                    queue.remove(Peer);
                    queue.add(new Pair<>(Peer.getValue0(), Peer.getValue1() + 1));
                    break;
                }
            }
            helpQueue = queue;
        }

        return Candidates_Map;
    }

    //In terminate, we must select peers that you want to give to the partitions that you are
    //  responsible for. After selecting them you upload your weights, publish a NEW_PEERS message and terminate .
    //Upon receiving the NEW_PEERS message, check if peer selected you,and then take the responsibility
    public void terminate() throws Exception {
        Map<Integer,String> candidate_peers = Find_Candidates();
        Multihash hash;
        List<String> Peers = new ArrayList<>();
        List<Integer> Auth = new ArrayList<>(candidate_peers.keySet());
        for(int i = 0; i < Auth.size(); i++){
            Peers.add(candidate_peers.get(Auth.get(i)));
        }
        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            ipfsClass.send(String.valueOf(PeerData.Auth_List.get(i)),ipfsClass.JOIN_PARTITION_SERVER(PeerData._ID,PeerData.Auth_List.get(i),(short)1));
        }
        hash = save_model();
        Peers.add(hash.toString());
        Peers.add(PeerData._ID);
        ipfsClass.send("New_Peer",ipfsClass.Marshall_Packet(Auth,Peers));
        Thread.sleep(5000);
        System.exit(1);
    }

    public static List<Double> read_file( String path) throws Exception{
        List<Double> arr = new ArrayList<>();
        FileInputStream fd = new FileInputStream(path);
        DataInputStream in = new DataInputStream(fd);
        for(int i= 0; i < PeerData._MODEL_SIZE; i++){
            arr.add(in.readDouble());
        }
        return arr;
    }

    /* Discover an IPFS node by using the dedicated discovery topic */
    public String getIPFSNode() throws Exception {
        /* clear the queue from the previous call */
        PeerData.ipfsNodesQueue.clear();

        ByteBuffer buf = ByteBuffer.allocate(PeerData._ID.length() + 2);
        buf.putShort(Constants.MessageTags.Discover);
        buf.put(PeerData._ID.getBytes());
        ipfs.pubsub.pub(Constants.Discovery.Topic, Base64.getUrlEncoder().encodeToString(buf.array()));
        /* Wait for ipfs nodes to reply
        Another thread handles these and puts their IDs to ipfsNodesQueue */
        return PeerData.ipfsNodesQueue.take();
    }

    //Main will become init method for the IPLS API
    //Here the peer will first interact with the other
    //peers in the swarm and take the responsibilities
    public void init() throws Exception {
        int i = 0;
        PeerData.dlog = new DebugInfo(false);
        //This is a list showing which partitions are missing from PeerData
        // in the initialization phase, in order to put them in our authority
        // list
        List<Integer> OrphanPartitions = new ArrayList<>();
        org.javatuples.Pair<String,Integer> tuple;
        InitializeWeights();
        for (i = 0; i < _PARTITIONS; i++) {
            PeerData.Partition_Availability.put(i,new ArrayList<>());
            PeerData.previous_iter_active_workers.put(i,(double)_PARTITIONS);
            PeerData.workers.put(i,new ArrayList<>());
            PeerData.Clients.put(i,new ArrayList<>());
            PeerData.New_Clients.put(i,new ArrayList<>());
            PeerData.New_Members.put(i,new ArrayList<>());
            PeerData.Replica_holders.put(i,new ArrayList<>());
            PeerData.New_Replicas.put(i,new ArrayList<>());
            PeerData.Committed_Hashes.put(i,new ArrayList<>());
            PeerData.Participants.put(i,1);
        }
        PeerData.Partitions_committed.put(0,new ArrayList<>());
        PeerData.Partitions_committed.put(1,new ArrayList<>());
        PeerData.Partitions_committed.put(2,new ArrayList<>());
        //Each peer gets in init phase an authority list in which he
        // subscribes in order to get gradients
        //List<Integer> Auth_List = new ArrayList<Integer>();
        List<String> BootstrapRequest = new ArrayList<String>();
        List<Peer> peers = new ArrayList<Peer>();

        ipfsClass = new MyIPFSClass(PeerData.Path);
        ipfs = new IPFS(PeerData.Path);
        commit = new IPLS_Comm(PeerData.Path);

        PeerData._ID = ipfs.id().get("ID").toString();

        PeerData.dlog.log("ID : " + PeerData._ID);
        PeerData.dlog.log("IPFS version : " + ipfs.version());

        // If there are no peers then leave abort, because in reality you
        // should know at least one peer, the bootstraper
        try {

            peers = ipfs.swarm.peers();
            PeerData.dlog.log("PEERS : "  + peers);

            for(i = 0; i < peers.size(); i++){
                PeerData.Existing_peers.add(peers.get(i).id.toString());
            }
        }
        catch(Exception e){
            PeerData.dlog.log("Peers not found ");
            peers = null;
        }

        
        //List<Double> Lmodel = read_file(FileName);
        List<Double> Lmodel = read_file(System.getProperty("user.home") + "/IPLS/" + FileName);
        System.out.println("Read File");
        //PeerData._MODEL_SIZE = Lmodel.size();
        InitializeWeights(Lmodel);
        System.out.println("Initialized Weights");
        //if(PeerData.Indirect_Communication){
        PeerData.storage_client = new DStorage_Client();
        PeerData.storage_client.start();
        Thread.sleep(1000);
        String[] View = PeerData.storage_client.get_Storage_View();
        if(View.length == 0){
            System.out.println(" No view available");
            System.exit(1);
        }
        int randomNum = ThreadLocalRandom.current().nextInt(0, View.length);
        System.out.println("View len : " + View.length + " random : " + randomNum);
        PeerData.Provider = View[randomNum];

        //}
        // Start updates_download_scheduler,aggregation_download_scheduler thread and directory service thread
        PeerData.ds_client = new IPLS_DS_Client(ipfs);
        PeerData.ds_client.start();

        PeerData.updates_download_scheduler = new Download_Scheduler(2);
        PeerData.updates_download_scheduler.start();
        if(PeerData._MIN_PARTITIONS != 0) {
            PeerData.aggregation_download_scheduler = new Download_Scheduler(0);
            PeerData.aggregation_download_scheduler.start();
            PeerData.partial_updates_download_scheduler = new Download_Scheduler(1);
            PeerData.partial_updates_download_scheduler.start();
        }


        if(PeerData.Indirect_Communication){
            PeerData.gradients_query_manager = new DS_query_manager( 0);
            PeerData.gradients_query_manager.start();
            if(!PeerData.fast_sync){
                PeerData.partial_updates_query_manager = new DS_query_manager(1);
                PeerData.partial_updates_query_manager.start();
            }
        }



        //Start _New_Peer thread in order to get new_peer messages
        _New_Peer = new ThreadReceiver(PeerData.Path);
        _New_Peer.start();
        PeerData.InitSem.acquire();
        
        //Start Personal_Thread to get personal messages
        _Personal_Thread = new ThreadReceiver(ipfs.id().get("ID").toString(),PeerData.Path,_PARTITIONS,_MIN_PARTITIONS);
        _Personal_Thread.start();
        PeerData.InitSem.acquire();
        
        _Auth_Listener = new ThreadReceiver("Authorities",PeerData.Path,_PARTITIONS,_MIN_PARTITIONS);
      
        _Auth_Listener.start();
        PeerData.InitSem.acquire();

        
        if(PeerData.isBootsraper){
            Sub thread;
            for(i = 0; i < PeerData._PARTITIONS; i++) {
                thread = new Sub(new Integer(i).toString(),PeerData.Path,PeerData.GGP_queue,true);
                thread.start();
                UpdaterThread = new Updater();
                UpdaterThread.start();
                PeerData.dlog.log("Updater Started...");
            }
            GlobalGradientPool GP_Thread = new GlobalGradientPool();
            GP_Thread.start();

            return;
        }

        // Until now, all threads have been created, and the peer is ready to communicate with the IPLS system.

        // Again leave in case no peers found in the IPFS private network
        if(peers == null){
            PeerData.dlog.log("Error, IPLS could not find any peers. Aborting ...");
            System.exit(-1);
        }

        //First, send a request to bootstrappers and ask them to give
        // you peer information in order to connect them into your swarm
        BootstrapRequest.add(ipfs.id().get("ID").toString());
        for(i = 0; i < peers.size(); i++){
            if(PeerData.Bootstrapers.contains(PeerData.Existing_peers.get(i))){
                PeerData.Wait_Ack.add(new Triplet<>(PeerData.Existing_peers.get(i),0,0));
                PeerData.dlog.log(PeerData.Existing_peers.get(i));
                ipfsClass.send(PeerData.Existing_peers.get(i),ipfsClass.Marshall_Packet(BootstrapRequest,false,(short) 7));
                PeerData.dlog.log("Sent");
            }
        }
        while(PeerData.Wait_Ack.size() != 0){Thread.yield();}
        // Upon communicating with all the possible bootstrappers the peer discovered as many new peers as possible,
        // and adds them to the Existing_peers list
        try {
            peers = ipfs.swarm.peers();
            for(i = 0; i < peers.size(); i++){
                if(PeerData.Existing_peers.contains(peers.get(i).id.toString()) == false){
                    PeerData.Existing_peers.add(peers.get(i).id.toString());
                }
            }
        }
        catch(Exception e){
            peers = null;
        }

        //Broadcast a message containing your multiaddress to the IPFS private network. Upon receiving
        // this message, each peer responds with the partitions that he is responsible for.
        BootstrapRequest.add(PeerData.MyPublic_Multiaddr);
        ipfsClass.send("New_Peer",ipfsClass.Marshall_Packet(BootstrapRequest,false,(short)7));

        PeerData.dlog.log("Waitting 5sec");
        Thread.sleep(5000);

        if(peers != null){
            // Wait in case you have received replies from all peers
            // in Existing_peers list. Otherwise continue.
            for(i = 0; i < 2; i++){
                Thread.sleep(2000);
                if(received_from_all_peers()){
                    break;
                }
                PeerData.dlog.log("Waiting Another 2sec");
            }
            if(PeerData.Relaxed_SGD){
                // Wait until the current IPLS round completes. This happens because the peer
                // might have entered in the middle of the current IPLS round/
                ipfsClass.wait_next_iter();
            }
            // There might be the possibility that you didn't find peers responsible for some
            // partitions. Those are called OrphanPartitions
            OrphanPartitions = Find_Orphan_Partitions();
            // If OrphanPartitions exist, become responsible for those partitions.
            if(OrphanPartitions.size() != 0){
                PeerData.dlog.log("GAP DETECTED : " + OrphanPartitions );
                //In case we have concurrent joins
                for(i = 0; i < OrphanPartitions.size(); i++){
                    PeerData.Auth_List.add(OrphanPartitions.get(i));
                    PeerData.Clients.put(OrphanPartitions.get(i),new ArrayList<>());
                }
                ipfsClass.send("Authorities",ipfsClass.Marshall_Packet(PeerData.Auth_List,null,ipfs.id().get("ID").toString(),(short) 2));
            }
            //Otherwise select some partitions and publish your responsibilities
            else{
                select_partition();
            }
        }
        else{
            // If no other peer found except from bootstrappers, be responsible for all partitions
            if(PeerData.Relaxed_SGD){
                ipfsClass.wait_next_iter();
            }

            for(i = 0; i < _PARTITIONS; i++){
                PeerData.Auth_List.add(i);
                PeerData.Clients.put(i,new ArrayList<>());
            }
        }


        SwarmManagerThread = new SwarmManager();
        SwarmManagerThread.start();
        PeerData.dlog.log("Swarm Manager Started...");
        UpdaterThread = new Updater();
        UpdaterThread.start();
        PeerData.dlog.log("Updater Started...");

        //----------------------------------------------------//
        List<Double> VectorModel = new ArrayList<>();
        for(i = 0; i < PeerData._PARTITIONS; i++){
            for(int j = 0; j < PeerData.Weights.get(i).length; j++){
                VectorModel.add(PeerData.Weights.get(i)[j]);
            }
        }

        // Initialize an IPLS directory where the updates and gradient partitions are
        // going to be saved.
        ipfsClass.initialize_IPLS_directory();

        File f = new File(ipfs.id().get("ID").toString()+"ETHModel");
        f.createNewFile();
        FileOutputStream fos = new FileOutputStream(ipfs.id().get("ID").toString()+"ETHModel");
        ObjectOutputStream oos = new ObjectOutputStream(fos);

        oos.writeObject(VectorModel);
        oos.close();
        fos.close();

        GlobalGradientPool GP_Thread = new GlobalGradientPool();
        GP_Thread.start();
        if(!PeerData.training_phase) {
            ipfsClass.send("New_Peer",ipfsClass._START_TRAINING());

        }

        // Wait until a predetermined number of peers is collected in order to proceed to
        // collaborative learning phase
        System.out.println("Waiting");
        while(!PeerData.training_phase){Thread.yield();}
        System.out.println("Schedule found");
        // Wait to download the Schedule_Hash
        while(PeerData.Schedule_Hash == null || ipfsClass.find_iter() == -1){
            PeerData.dlog.log("No schedule found");
            Thread.sleep(1000);
        }
        PeerData.dlog.log(PeerData.Schedule_Hash);
        PeerData.mtx.acquire();
        if(ipfsClass.find_iter() != 0){
            //for(i = 0; i < PeerData._PARTITIONS ; i++){
            //    if(PeerData.Auth_List.contains(i)){
             //       PeerData.Wait_Ack.remove(new org.javatuples.Triplet<>("First_Iter", i,ipfsClass.find_iter()-1));
             //   }
            //}
            retrieve_updates(ipfsClass.find_iter()-1);
        }
        PeerData.middleware_iteration = ipfsClass.find_iter();
        PeerData.mtx.release();
    }
}
