import io.ipfs.api.Peer;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;

import javax.crypto.SecretKey;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class Download_Scheduler extends Thread{
    MyIPFSClass ipfs;
    boolean mode;
    BlockingQueue<Quartet<String,String,Integer,Integer>> comm_queue = new LinkedBlockingQueue<>();
    BlockingQueue<Quartet<String,String,Integer,Integer>> update_queue = new LinkedBlockingQueue<>();


    public Download_Scheduler(boolean mode){
        ipfs = new MyIPFSClass(PeerData.Path);
        this.mode = mode;
    }



    public void update_peer_download_time(int time,String Peer){
        if(PeerData._PEER_DOWNLOAD_TIME.containsKey(Peer)){
            PeerData._PEER_DOWNLOAD_TIME.put(Peer, PeerData._PEER_DOWNLOAD_TIME.get(Peer) + time);
        }
        else{
            PeerData._PEER_DOWNLOAD_TIME.put(Peer , time);
        }
    }

    //Downloading files in IPFS has a major problem : * Whenever the providers of a file are disconnected for the internet,
    // and a peer wants to download this file, he has to block all the time until he downloads the entirity of the file.
    // * The sane holds true whenever a peer downloads a file but suddenly all the provides get disconnected from the internet.
    // I solve this problem like this : When the aggregator wants to download a update partition, he first of all checks if at
    // least one provider is connected to the network. Then starts downloading the file, until timeout elapses. If the file
    // is not entirely downloaded then function ## download_updates ## will return 1. If the file is fully downloaded, the
    // function will return 0. If there are no providers, the function will return 2.
    public short download_updates(String Hash,String Peer) throws Exception{
        int _DOWNLOAD_START_TIME = 0,_DOWNLOAD_END_TIME = 0;

        boolean downloaded = false;
        if(ipfs.has_providers(Hash)){
            _DOWNLOAD_START_TIME = (int) Instant.now().getEpochSecond();
            if(ipfs.Download_Updates(Hash)){
                _DOWNLOAD_END_TIME = (int)Instant.now().getEpochSecond();
                update_peer_download_time(_DOWNLOAD_END_TIME-_DOWNLOAD_START_TIME,Peer);
                return 0;
            }
            else{
                _DOWNLOAD_END_TIME = (int)Instant.now().getEpochSecond();
                update_peer_download_time(_DOWNLOAD_END_TIME-_DOWNLOAD_START_TIME,Peer);
                PeerData._LOG.get("download_times").add(new Triplet<String,Integer,Integer>(Peer,PeerData.middleware_iteration,PeerData._PEER_DOWNLOAD_TIME.get(Peer)));
                return 1;
            }
        }
        return 2;
    }

    public void schedule(List<Quartet<String,String,Integer,Integer>> committed_hashes) throws Exception{
        PeerData._LOG.put("download_times",new ArrayList<>());
        Quartet<String,String,Integer,Integer> commitment;
        List<Quartet> black_list = new ArrayList<>();
        for(int i = 0; i < committed_hashes.size(); i++){
            commitment = committed_hashes.get(i);
            int Partition = commitment.getValue3();
            String Hash = commitment.getValue0();
            String Peer = commitment.getValue1();
            int iter = commitment.getValue2();
            if(download_updates(Hash,Peer) == 0){
                black_list.add(commitment);
                System.out.println("Gradients downloaded " + Peer + " , P : " + Partition + " , I : " + iter);
                // When a partition of gradients of a peer is fully downloaded, check if there is
                // its key pair in order to decrypt it (peers send their keys upon begin of aggregation phase).
                // If there is not any key yet then add the downloaded hash to the Downloaded_Hashes data structure.

                PeerData.com_mtx.acquire();
                if(PeerData.Hash_Keys.containsKey(Hash)){
                    PeerData.queue.add(new Quintet<>(Peer,Partition,iter,true,(List<Double>) ipfs.decrypt(ipfs.Get_bytes(Hash),PeerData.Hash_Keys.get(Hash))));
                    PeerData.Hash_Keys.remove(Hash);
                }
                else{
                    PeerData.Downloaded_Hashes.add(Hash);
                }
                PeerData.com_mtx.release();

            }
        }
        for(int i = 0; i < black_list.size(); i++){
            PeerData.downloaded_hashes++;
            committed_hashes.remove(black_list.get(i));
        }
        black_list = null;
    }

    // This is the download scheduler of the updates. The main responsibility of this module, is to download the updates
    // of the peers and store them locally. The downloader might not download all the file of a given update at once
    // (download_updates returns 1), and he might need to call this function more than once, continuing from the point
    // that he timed out.
    public void select_updates() throws Exception{
        List<Quartet<String,String,Integer,Integer>> Committed_hashes = new ArrayList<>();
        while(true){
            if(comm_queue.size() == 0 && Committed_hashes.size() == 0 ){
                Committed_hashes.add(comm_queue.take());
                PeerData.commited_hashes++;
            }
            while(comm_queue.size() != 0){
                Committed_hashes.add(comm_queue.take());
                PeerData.commited_hashes++;
            }
            if(ipfs.find_iter() == -1){
                Thread.sleep(100);
                continue;
            }
            if( ipfs.get_curr_time() < ipfs.aggregation_elapse_time(ipfs.find_iter()) ){
                schedule(Committed_hashes);
            }
            else {
                while(comm_queue.size() != 0){
                    comm_queue.take();
                }
                Committed_hashes = new ArrayList<>();
            }
        }
    }

    public void add_file(Quartet commitment){
        comm_queue.add(commitment);
    }
    /*
    public void select_updates(){
        int downloaded = 0;
        for(int i = 0; i < PeerData.Committed_Hashes.get(Partition).size(); i++){
            String Hash = PeerData.Committed_Hashes.get(Partition).get(i).getValue0();
            String Peer = PeerData.Committed_Hashes.get(Partition).get(i).getValue1();
            int iter = PeerData.Committed_Hashes.get(Partition).get(i).getValue2();
            List<Double> Gradients = null;

            try {
                if(!Hash.equals("N")){
                    Gradients = (List<Double>) ipfs.DownloadParameters(Hash);
                    downloaded++;
                }
            }
            catch (Exception e){
                System.out.println("Could not download Parameters");
                continue;
            }
            //Check time
            PeerData.queue.add(new Quintet<>(Peer,Partition,iter,true,Gradients));
            if(ipfs.aggregation_elapse_time(PeerData.middleware_iteration) < ipfs.get_curr_time()){
                try {
                    PeerData.mtx.acquire();
                    PeerData.used_commitments += downloaded;
                    PeerData.mtx.release();
                }
                catch (Exception e){}
                System.out.println("Could not download all Gradients");
                PeerData.Committed_Hashes.put(Partition ,new ArrayList<>());
                return;
            }

        }

        try {
            PeerData.mtx.acquire();
            PeerData.used_commitments += downloaded;
            PeerData.mtx.release();
        }
        catch (Exception e){}
        PeerData.Committed_Hashes.put(Partition ,new ArrayList<>());
    }


     */

    public void cache_partition(String Origin_Peer, int Partition, int Iteration, List<Double> Updated_Partition) throws Exception{
        Triplet<String, Integer, Integer> pair = new Triplet<>(Origin_Peer, Partition, Iteration);
        for (int i = 0; i < PeerData.Weight_Address.get(Partition).size(); i++) {
            PeerData.Weight_Address.get(Partition).set(i, Updated_Partition.get(i));
        }
        //PeerData.Weight_Address.put(ReplyPair.getValue1(),ReplyPair.getValue2());
        PeerData.mtx.acquire();

        // Check the iteration number of each server. Each peer in the system must be synchronized
        // But by some reason some peers may meet you in iteration n and others in iteration n+1
        if (PeerData.Servers_Iteration.containsKey(Origin_Peer)) {
            if (PeerData.Servers_Iteration.get(Origin_Peer) < Iteration) {
                PeerData.Servers_Iteration.replace(Origin_Peer, Iteration);
            }
        } else {
            PeerData.Servers_Iteration.put(Origin_Peer, Iteration);
        }
        //if(PeerData.middleware_iteration > 0 && ReplyPair.getValue2() == 0){
        //    ipfs.pubsub.pub(ReplyPair.getValue0(),AuxilaryIpfs.Marshall_Packet(PeerData.Weights.get(ReplyPair.getValue1()),ipfs.id().get("ID").toString(),ReplyPair.getValue1(),PeerData.middleware_iteration,(short)3));
        //}
        if (PeerData.Wait_Ack.contains(pair)) {
            PeerData.Wait_Ack.remove(pair);

        } else if (PeerData.Wait_Ack.contains(new Triplet<>(Origin_Peer, Partition, PeerData.middleware_iteration))) {
            if (PeerData.middleware_iteration < Iteration) {
                PeerData.Wait_Ack.remove(new Triplet<>(Origin_Peer, Partition, PeerData.middleware_iteration));
                PeerData.Wait_Ack_from_future.add(pair);
            }
        } else if (PeerData.Wait_Ack.contains(new Triplet<>(Origin_Peer, Partition, -1))) {
            PeerData.Wait_Ack.remove(new Triplet<>(Origin_Peer, Partition, -1));
        } else {
            PeerData.Wait_Ack_from_future.add(pair);
        }
        PeerData.mtx.release();

    }

    public void schedule_updates(List<Quartet<String,String,Integer,Integer>> Update_hashes) throws Exception{
        Quartet<String,String,Integer,Integer> update;
        List<Quartet> black_list = new ArrayList<>();
        List<Double> Updated_partition = new ArrayList<>();
        for(int i = 0; i < Update_hashes.size(); i++){
            update = Update_hashes.get(i);
            int Partition = update.getValue3();
            String Hash = update.getValue0();
            String Peer = update.getValue1();
            int iter = update.getValue2();
            int status = download_updates(Hash,Peer);
            if(iter < PeerData.middleware_iteration){
                System.out.println("PUT in blacklist");
                black_list.add(update);
                continue;
            }
            if(status == 0){
                black_list.add(update);
                System.out.println("Downloading Updated Partition : " + Partition + " at : " + ipfs.get_curr_time());
                Updated_partition = (List<Double>) ipfs.DownloadParameters(Hash);
                cache_partition(Peer,Partition,iter,Updated_partition);
                PeerData.downloaded_updates++;
            }
            else{
                if(status == 1){
                    System.out.println("Failed to complete download of : " + Partition);
                }
                else{
                    System.out.println("No providers found");
                }
            }
        }
        for(int i = 0; i <black_list.size(); i++){
            Update_hashes.remove(black_list.get(i));
        }
        black_list = null;
    }

    public void get_updated_partitions() throws Exception{
        // String : file hash , String : Peer ID, Integer : iter , Integer : Partition
        List<Quartet<String,String,Integer,Integer>> Update_hashes = new ArrayList<>();
        Quartet<String,String,Integer,Integer> update_tuple;
        while(true){
            if(update_queue.size() == 0 && Update_hashes.size() == 0 ){
                update_tuple = update_queue.take();
                if(update_tuple.getValue2() == PeerData.middleware_iteration){
                    Update_hashes.add(update_tuple);
                }
            }
            while(update_queue.size() != 0){
                update_tuple = update_queue.take();
                if(update_tuple.getValue2() == PeerData.middleware_iteration){
                    Update_hashes.add(update_tuple);
                }
            }

            if(Update_hashes.size() != 0 && (ipfs.training_elapse_time(PeerData.middleware_iteration + 1) == -1 ||
                    (ipfs.training_elapse_time(PeerData.middleware_iteration + 1) != -1 &&
                            ipfs.get_curr_time() < ipfs.training_elapse_time(PeerData.middleware_iteration+1) - ipfs.get_training_time()/6))){
                schedule_updates(Update_hashes);
            }
            else{
                Update_hashes = new ArrayList<>();
                while(update_queue.size() != 0){
                    update_queue.take();
                }
            }
        }

    }

    public void add_update(Quartet<String, String, Integer, Integer> update){
        System.out.println("Got new update " + ipfs.get_curr_time());
        update_queue.add(update);
    }

    public void run() {
        if(mode){
            try {select_updates();}
            catch (Exception e){System.out.println(e);}

        }
        else{
            try {get_updated_partitions();}
            catch (Exception e){System.out.println(e);}
        }
    }
}
