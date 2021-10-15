import com.sun.xml.internal.ws.wsdl.writer.document.Part;
import io.ipfs.api.Peer;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;
import org.web3j.abi.datatypes.Int;

import javax.crypto.SecretKey;
import java.awt.*;
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
    BlockingQueue<Quintet<String,String,Integer,Integer,String>> comm_queue = new LinkedBlockingQueue<>();
    BlockingQueue<Quartet<String,String,Integer,Integer>> update_queue = new LinkedBlockingQueue<>();
    List<Quintet<String,String,Integer,Integer,String>> Other_Peers_commitments = new ArrayList<>();

    Map<Integer,List<Quartet<String,String,Integer,Integer>>> Updates_Collection = new HashMap<>();

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

    // This method is called in order to download the content from the given list. If
    // no provider found for all the gradients return false, else return true
    public boolean download_gradients(List<Quintet<String,String,Integer,Integer,String>> committed_hashes) throws Exception{
        Quintet<String,String,Integer,Integer,String> commitment;
        List<Quintet> black_list = new ArrayList<>();
        boolean providers_exist = false;
        for(int i = 0; i < committed_hashes.size() && ipfs.aggregation_elapse_time(ipfs.find_iter()) > ipfs.get_curr_time() ; i++){
            commitment = committed_hashes.get(i);
            int Partition = commitment.getValue3();
            String Hash = commitment.getValue0();
            String Peer = commitment.getValue1();
            String Aggregator = commitment.getValue4();
            if(!PeerData._ID.equals(Aggregator) && PeerData.Received_Replicas.contains(new Pair<>(Partition,Aggregator))){
                PeerData.Other_Replica_Gradients.remove(new Pair<>(Partition,Aggregator));
                PeerData.Other_Replica_Gradients_Received.remove(new Pair<>(Partition,Aggregator));
                black_list.add(commitment);
                continue;
            }
            int iter = commitment.getValue2();
            int status = download_updates(Hash,Peer);
            if((status == 1 || status == 0) && !providers_exist ){
                providers_exist = true;
            }
            if(status == 0 ){
                black_list.add(commitment);
                if(Aggregator.equals(PeerData._ID)){
                    PeerData.downloaded_hashes++;
                    System.out.println("Gradients downloaded " + Peer + " , P : " + Partition + " , I : " + iter);
                }
                else{
                    System.out.println("Other aggregators gradients downloaded " + Aggregator + " , P : " + Partition + " , I : " + iter);
                }

                // When a partition of gradients of a peer is fully downloaded, check if there is
                // its key pair in order to decrypt it (peers send their keys upon begin of aggregation phase).
                // If there is not any key yet then add the downloaded hash to the Downloaded_Hashes data structure.
                PeerData.com_mtx.acquire();
                if(PeerData.Hash_Keys.containsKey(Hash)){
                    PeerData.com_mtx.release();

                    // In case you can open the commitment, and the gradients are destined for the aggregator, aggregate the data
                    if(PeerData._ID.equals(Aggregator)){
                        PeerData.queue.add(new Quintet<>(Peer,Partition,iter,true,(List<Double>) ipfs.decrypt(ipfs.Get_bytes(Hash),PeerData.Hash_Keys.get(Hash))));
                    }
                    // If the aggregator downloaded other aggregators date, store them in the Other_Replica_Gradients. This data
                    // structure is important in the case where the replica does not respond in the synchronization phase and thus
                    // do not lose some amount of gradients.
                    else{
                        PeerData.com_mtx.acquire();
                        if(PeerData.Received_Replicas.contains(new Pair<>(Partition,Aggregator))){
                            PeerData.com_mtx.release();
                        }
                        else if(PeerData.Other_Replica_Gradients.containsKey(new Pair<>(Partition,Aggregator))){

                            List<Double> gradients = (List<Double>) ipfs.decrypt(ipfs.Get_bytes(Hash),PeerData.Hash_Keys.get(Hash));
                            for(int j = 0; j < gradients.size(); j++){
                                PeerData.Other_Replica_Gradients.get(new Pair<>(Partition,Aggregator)).set(j,PeerData.Other_Replica_Gradients.get(new Pair<>(Partition,Aggregator)).get(j) + gradients.get(j));
                            }
                            gradients = null;
                            PeerData.Other_Replica_Gradients_Received.put(new Pair<>(Partition,Aggregator),PeerData.Other_Replica_Gradients_Received.get(new Pair<>(Partition,Aggregator))+1);
                            PeerData.com_mtx.release();
                        }
                        else{
                            PeerData.Other_Replica_Gradients.put(new Pair<>(Partition,Aggregator),(List<Double>) ipfs.decrypt(ipfs.Get_bytes(Hash),PeerData.Hash_Keys.get(Hash)));
                            PeerData.Other_Replica_Gradients_Received.put(new Pair<>(Partition,Aggregator),1);
                            PeerData.com_mtx.release();
                        }

                    }
                    PeerData.Hash_Keys.remove(Hash);
                }
                else{
                    PeerData.Downloaded_Hashes.put(Hash,Aggregator);
                    PeerData.com_mtx.release();
                }

            }
        }
        for(int i = 0; i < black_list.size(); i++){
            committed_hashes.remove(black_list.get(i));
        }
        black_list = null;
        return providers_exist;
    }

    // Given a list of hashes, download all them or some part of them. Also in case the aggregation phase is finished return
    // Note that the scheduler might download half of the content of gradients at one time and the other half later.
    public void schedule(List<Quintet<String,String,Integer,Integer,String>> committed_hashes) throws Exception{
        PeerData._LOG.put("download_times",new ArrayList<>());
        // in case there was no provider in the gradients that the aggregator
        // has to download, start downloading other aggregators for the same
        // partitions gradients
        if(!download_gradients(committed_hashes) &&
                ipfs.training_elapse_time(ipfs.find_iter()) < ipfs.get_curr_time() &&
                ipfs.get_curr_time() < ipfs.aggregation_elapse_time(ipfs.find_iter())){
            download_gradients(Other_Peers_commitments);
        }

    }

    // This is the download scheduler of the updates. The main responsibility of this module, is to download the updates
    // of the peers and store them locally. The downloader might not download all the file of a given update at once
    // (download_updates returns 1), and he might need to call this function more than once, continuing from the point
    // that he timed out.
    public void select_updates() throws Exception{
        List<Quintet<String,String,Integer,Integer,String>> Committed_hashes = new ArrayList<>();
        while(true){

            // Wait until you get new commitmets. For efficiency purposes you can download the content of the commitment
            // before aggregation phase starts, but the content is encrypted until the start of the aggregation phase.
            // Also you can't get any commitment in the aggregation phase.
            while(comm_queue.size() == 0 && Committed_hashes.size() == 0 ){
                Thread.sleep(200);
                if(ipfs.find_iter() == -1){
                    continue;
                }
                if(comm_queue.size() > 0){
                    break;
                }
                if(ipfs.training_elapse_time(ipfs.find_iter()) < ipfs.get_curr_time()){
                    break;
                }
            }
            // Get every commitment the peer received
            while(comm_queue.size() != 0){
                Committed_hashes.add(comm_queue.take());
                PeerData.commited_hashes++;
            }

            // In case the peer did not received the up to date schedule yet
            while(ipfs.find_iter() == -1){
                Thread.sleep(100);
            }

            // If aggregation time hasn't finished yet download some content
            if( ipfs.get_curr_time() < ipfs.aggregation_elapse_time(ipfs.find_iter()) && Committed_hashes.size() != 0){
                schedule(Committed_hashes);
            }
            // If an aggregator has received all the gradients from his "clients", then he
            // can download as many gradients he can from other aggregators (of the same partition)
            // clients
            else if(ipfs.get_curr_time() > ipfs.training_elapse_time(ipfs.find_iter()) &&
                    ipfs.get_curr_time() < ipfs.aggregation_elapse_time(ipfs.find_iter()) &&
                    Other_Peers_commitments.size()!= 0 &&
                    PeerData.commited_hashes == PeerData.downloaded_hashes){
                for(int i = 0; i < Other_Peers_commitments.size(); i++){
                    Committed_hashes.add(Other_Peers_commitments.get(i));
                }
                Other_Peers_commitments = new ArrayList<>();
            }
            // In case aggregation phase finished remove everything and wait for the new iterations
            // commitments
            else {
                while(comm_queue.size() != 0){
                    comm_queue.take();
                }
                Committed_hashes = new ArrayList<>();
                Other_Peers_commitments = new ArrayList<>();
            }
        }
    }

    // When a peer receives a commitment checks if it is from his own "client"
    // or others peers client (responsible for the same partition)
    public void add_file(Quintet commitment){
        if(commitment.getValue4().equals(PeerData._ID)){
            System.out.println("Get clients commitment");
            comm_queue.add(commitment);
        }
        else{
            System.out.println("Get other aggregators clients commitments");
            Other_Peers_commitments.add(commitment);
        }
    }

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
        }
        else if (PeerData.Wait_Ack.contains(new Triplet<>(Origin_Peer, Partition, PeerData.middleware_iteration))) {
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

    // This method takes as input an update and depending on updates download policy,
    // updates the data-structures. The policy used for now is FIFO by which i mean
    // that the peer downloads the first update for the partition he receives. However
    // he keeps the other different hashes for the same partition in case of emergency.
    public void process_update(Quartet<String,String,Integer,Integer> update) throws Exception{
        if(update.getValue2() < ipfs.find_iter() - 1){
            return;
        }
        Updates_Collection.get(update.getValue3()).add(update);
        System.out.println("Update added");
    }


    // This method selects the fitting update data for download given the download
    // policy. For example for FIFO policy it returns the first one of all the
    // available updates for the same partition.
    public Quartet<String,String,Integer,Integer> select_provider(int partition){
        if(Updates_Collection.get(partition).size() == 0){
            return null;
        }
        return Updates_Collection.get(partition).get(0);
    }



    public void schedule_updates(List<Integer> Downloaded_partitions) throws Exception{
        Quartet<String,String,Integer,Integer> update;
        List<Double> Updated_partition = new ArrayList<>();
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            // If the partition has already by downloaded or exists locally, continue
            if(PeerData.Auth_List.contains(i) || Downloaded_partitions.contains(i)){
                continue;
            }
            // For each partition, select the hash of its update to download
            update = select_provider(i);

            // if no hash found continue to next partition
            if(update == null){
                continue;
            }

            int Partition = update.getValue3();
            String Hash = update.getValue0();
            String Peer = update.getValue1();
            int iter = update.getValue2();
            // Download the content
            int status = download_updates(Hash,Peer);
            // if the file was fully downloaded then add partition i to the downloaded partitions, cache the
            // partition so that it can be retrieved by GetPartitions
            if(status == 0){
                Downloaded_partitions.add(i);
                System.out.println("Downloaded Update : " + Partition);
                Updated_partition = (List<Double>) ipfs.DownloadParameters(Hash);
                if(PeerData.Dealers.containsKey(i)){
                    cache_partition(PeerData.Dealers.get(i),Partition,iter,Updated_partition);
                }
                else{
                    cache_partition("First_Iter",Partition,iter,Updated_partition);
                }
                PeerData.downloaded_updates++;
            }
            else{
                if(status == 1){
                    System.out.println("Failed to complete download of : " + Partition);
                }
                else{
                    Updates_Collection.remove(0);
                    Updates_Collection.get(i).add(update);
                    System.out.println("No providers found");
                }
            }
        }
        if(downloaded_all_partitions(Downloaded_partitions)){
            Updates_Collection = new HashMap<>();
            for(int i = 0; i < PeerData._PARTITIONS; i++){
                Downloaded_partitions.remove(new Integer(i));
            }
            for(int i = 0; i < PeerData._PARTITIONS; i++){
                Updates_Collection.put(i,new ArrayList<>());
            }
            while(update_queue.size() != 0){
                update_queue.take();
            }
        }
    }

    // Check if there partition updates not downloaded yet
    public boolean downloaded_all_partitions(List<Integer> Downloaded_partitions){
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            // If peer has recently joined the system then he has to download all the partitions
            // else he has to download the partitions that he isn't responsible for
            if(!PeerData.Auth_List.contains(i) &&
                    !Downloaded_partitions.contains(i)){
                return false;
            }
        }
        return true;
    }

    // This method checks if there exist hashes of updated partitions which haven't been
    // downloaded yet. If this is the case then the method returns false. Else it returns
    // true
    public boolean must_wait(List<Integer> Downloaded_partitions){
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            if(!PeerData.Auth_List.contains(i) && !Downloaded_partitions.contains(i) && Updates_Collection.get(i).size() > 0){
                return false;
            }
        }
        return true;
    }

    // This method checks if there are partitions that their aggregators didn't send
    // any hashes and thus could not receive the latest model.
    public List<Integer> check_unavailable_partitions(){
        List<Integer> unavailable_partitions = new ArrayList<>();
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            if(!PeerData.Auth_List.contains(i) && Updates_Collection.get(i).size() == 0){
                unavailable_partitions.add(i);
            }
        }
        return  unavailable_partitions;
    }

    // Check from all the updates if there some older updates that their
    // iteration number is smaller than the current iteration. Find them
    // and remove them so that there will be no incosistencies.
    public void clean_updates_collection() throws Exception{
        List<Quartet<String,String,Integer,Integer>> black_list = new ArrayList<>();
        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            for(int j = 0; j < Updates_Collection.get(i).size(); j++){
                if(Updates_Collection.get(i).get(j).getValue2() < ipfs.find_iter()){
                    black_list.add(Updates_Collection.get(i).get(j));
                }
            }
            for(int j = 0; j < black_list.size(); j++){
                Updates_Collection.remove(black_list.get(j));
            }
            black_list = new ArrayList<>();
        }
        black_list = null;
    }

    public void get_updated_partitions() throws Exception{
        // String : file hash , String : Peer ID, Integer : iter , Integer : Partition
        List<Quartet<String,String,Integer,Integer>> Update_hashes = new ArrayList<>();
        Quartet<String,String,Integer,Integer> update_tuple;
        List<Integer> Downloaded_partitions = new ArrayList<>();

        for(int i = 0; i < PeerData._PARTITIONS; i++){
            Updates_Collection.put(i,new ArrayList<>());
        }

        while(true){
            while (ipfs.find_iter() == -1){Thread.yield();}
            // In case the update_queue is empty and there aren't hashes of partitions
            // that haven't been downloaded yet then the scheduler must wait.
            if(update_queue.size() == 0 && must_wait(Downloaded_partitions)){
                update_tuple = update_queue.take();
                if(update_tuple.getValue2() >= ipfs.find_iter() - 1){
                    process_update(update_tuple);
                }
            }
            // Get all updates from update_queue
            while(update_queue.size() != 0){
                update_tuple = update_queue.take();
                if(update_tuple.getValue2() >= ipfs.find_iter() - 1){
                    process_update(update_tuple);
                }
            }
            // If peer didn't download all partitions yet, and he didn't pass the time limits, he can proceed
            // on downloading updated partitions.
            if(!downloaded_all_partitions(Downloaded_partitions) &&
                    ((ipfs.training_elapse_time(ipfs.find_iter()) == -1 ||
                    (ipfs.training_elapse_time(ipfs.find_iter()) != -1 &&
                     ipfs.get_curr_time() < ipfs.training_elapse_time(ipfs.find_iter()) - ipfs.get_training_time()/6)))){
                System.out.println("Start downloading");
                schedule_updates(Downloaded_partitions);
            }
            // Consider the case where the peer has joined the network, and hasn't yet began
            // training the model neither initialized his middleware_iteration. Thus the thread
            // hash to wait for the peer to initialize his middleware iteration first and then
            // clear older updates
            else if(PeerData.middleware_iteration == 0 && (ipfs.find_iter() == -1 || ipfs.find_iter() != 1)){
                while(PeerData.middleware_iteration != 0){
                    Thread.yield();
                }
                System.out.println("PROCEEDING " + Updates_Collection);
                clean_updates_collection();

            }
            else{
                System.out.println("Out of time");
                // Do something with unavailable partitions
                Updates_Collection = new HashMap<>();
                Downloaded_partitions = new ArrayList<>();
                for(int i = 0; i < PeerData._PARTITIONS; i++){
                    Updates_Collection.put(i,new ArrayList<>());
                }
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
