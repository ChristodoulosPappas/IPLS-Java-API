import io.ipfs.api.Peer;
import org.javatuples.*;
import org.web3j.abi.datatypes.Int;

import javax.crypto.SecretKey;
import java.awt.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;


public class Download_Scheduler extends Thread{
    MyIPFSClass ipfs;
    int hashes_remaining;
    int mod;
    BlockingQueue<Quintet<String,String,Integer,Integer,String>> comm_queue = new LinkedBlockingQueue<>();
    BlockingQueue<Quintet<String,List<String>,Integer,Integer,String>> merges_queue = new LinkedBlockingQueue<>();
    BlockingQueue<Quartet<String,String,Integer,Integer>> update_queue = new LinkedBlockingQueue<>();
    List<Quintet<String,String,Integer,Integer,String>> Other_Peers_commitments = new ArrayList<>();
    BlockingQueue<List<Quartet<String,String,Integer,Integer>>> batched_update_queue = new LinkedBlockingDeque<>();
    Map<Integer,List<Quartet<String,String,Integer,Integer>>> Updates_Collection = new HashMap<>();
    BlockingQueue<Quartet<String,String,Integer,Integer>> partial_updates_queue = new LinkedBlockingQueue<>();
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_RESET = "\u001B[0m";

    public Download_Scheduler(int mod){
        ipfs = new MyIPFSClass(PeerData.Path);
        this.mod = mod;
    }

    // Find all the providers for each block and also find the blocks on each provider
    public Map<String,List<String>> Find_storage_nodes(List<String> Block_Cids) throws Exception{
        List<String> provs = new ArrayList<>();
        // Maps each block cid to the coresponding providers
        Map<String,List<String>> providers_of_block = new HashMap<>();
        // Maps each the block cids each provider holds
        Map<String,List<String>> blocks_of_providers = new HashMap<>();
        for(int i = 0; i < Block_Cids.size(); i++){
            providers_of_block.put(Block_Cids.get(i),new ArrayList<>());
            provs = ipfs.find_providers(Block_Cids.get(i));
            if(provs.size() == 0){
                Thread.sleep(400);
                provs = ipfs.find_providers(Block_Cids.get(i));
            }
            for(int j = 0; j < provs.size(); j++){
                providers_of_block.get(Block_Cids.get(i)).add(provs.get(j));
            }
            for(int j = 0; j < provs.size(); j++){
                if(!blocks_of_providers.containsKey(provs.get(j))){
                    blocks_of_providers.put(provs.get(j),new ArrayList<>());
                }
                blocks_of_providers.get(provs.get(j)).add(Block_Cids.get(i));
            }
        }
        return blocks_of_providers;
    }

    // This method finds the set difference. (i.e [1,2,3] - [1,2,6] = [3])
    public List<String> set_difference(List<String> arr1,List<String> arr2){
        Set<String> a = new HashSet<>(arr1);
        Set<String> b = new HashSet<>(arr2);
        a.removeAll(b);
        return new ArrayList<>(a);
    }

    // This method decides what blocks are going to be partial aggregated and from whom provider.
    // It returns a map of [provider : [Cids]].
    public Map<String,List<String>>  decide_partial_aggregations(Map<String,List<String>> blocks_of_providers,List<String> Block_Cids){
        Map<String,List<String>> list_partial_aggregations = new HashMap<>();
        List<String> remaining_block_cids = Block_Cids;
        int provides;
        String best_provider=null;

        while(remaining_block_cids.size() != 0){
            List<String> providers = new ArrayList<>(blocks_of_providers.keySet());
            provides = 0;
            // find the best provider, the provider that stores the largest
            // number of blocks
            for(int i = 0; i < providers.size(); i++){
                if(blocks_of_providers.get(providers.get(i)).size() > provides){
                    best_provider = providers.get(i);
                    provides = blocks_of_providers.get(providers.get(i)).size();
                }
            }
            // Remove the blocks from the other peers
            for(int i = 0; i < providers.size(); i++){
                if(!providers.get(i).equals(best_provider)){
                    blocks_of_providers.replace(providers.get(i),set_difference(blocks_of_providers.get(providers.get(i)),blocks_of_providers.get(best_provider)));
                }
            }
            remaining_block_cids = set_difference(remaining_block_cids,blocks_of_providers.get(best_provider));
            list_partial_aggregations.put(best_provider,blocks_of_providers.get(best_provider));
            blocks_of_providers.remove(best_provider);
        }
        return list_partial_aggregations;
    }

    // Send merge request to all selected providers
    public void Merge_request(Map<String,List<String>> providers, int partition) throws Exception{
        List<String> providers_id = new ArrayList<>(providers.keySet());
        for(int i = 0; i < providers_id.size(); i++){
            ipfs.send(providers_id.get(i),ipfs.Marshall_Packet((short) 1, partition, ipfs.find_iter(),PeerData._ID, providers.get(providers_id.get(i))));
        }
    }

    // In reality, we could partially aggregate some gradient partitions, that are
    // stored on the same storage node, so that  we can download less data. For example
    // consider that we have to download gradients for the partition 1 : [g1[1],g2[1],g3[1],g4[1],g5[1]],
    // but g1[1],g2[1] and g3[1] are on the same storage node. Thus we can aggregate :
    // [g1[1] + g2[1] + g3[1],g4[1],g5[1]].
    public void Minimize_Gradients_file(List<String> Block_Cids, int partition) throws Exception{
        // find the best possible partial aggregation
        Map<String,List<String>> providers = decide_partial_aggregations(Find_storage_nodes(Block_Cids),Block_Cids);
        PeerData.dlog.log("Providers : " + providers);
        // notify the storage peers
        Merge_request(providers,partition);
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
        //if(ipfs.has_providers(Hash)){
        _DOWNLOAD_START_TIME = (int) Instant.now().getEpochSecond();
        if(ipfs.Download_Updates(Hash)){
            _DOWNLOAD_END_TIME = (int)Instant.now().getEpochSecond();
            if(Peer != null){
                update_peer_download_time(_DOWNLOAD_END_TIME-_DOWNLOAD_START_TIME,Peer);
            }
            return 0;
        }
        else{
            _DOWNLOAD_END_TIME = (int)Instant.now().getEpochSecond();
            if(Peer != null) {
                update_peer_download_time(_DOWNLOAD_END_TIME-_DOWNLOAD_START_TIME,Peer);
                PeerData._LOG.get("download_times").add(new Triplet<String,Integer,Integer>(Peer,PeerData.middleware_iteration,PeerData._PEER_DOWNLOAD_TIME.get(Peer)));
            }
            return 1;
        }
        //}
        //return 2;
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
            int iter = commitment.getValue2();
            if(!PeerData._ID.equals(Aggregator) && PeerData.Received_Replicas.contains(new Pair<>(Partition,Aggregator))){
                PeerData.Other_Replica_Gradients.remove(new Pair<>(Partition,Aggregator));
                PeerData.Other_Replica_Gradients_Received.remove(new Pair<>(Partition,Aggregator));
                black_list.add(commitment);
                continue;
            }
            else if(iter < ipfs.find_iter()){
                black_list.add(commitment);
                continue;
            }
            int status = download_updates(Hash,Peer);
            if((status == 1 || status == 0) && !providers_exist ){
                providers_exist = true;
            }
            if(status == 0 ){
                black_list.add(commitment);
                if(Aggregator.equals(PeerData._ID)){
                    PeerData.downloaded_hashes++;
                    PeerData.dlog.log("Gradients downloaded " + Peer + " , P : " + Partition + " , I : " + iter);
                }
                else{
                    PeerData.dlog.log("Other aggregators gradients downloaded " + Aggregator + " , P : " + Partition + " , I : " + iter);
                }

                PeerData.com_mtx.acquire();
                // In case you downloaded the data and the gradients are destined for the aggregator, aggregate the data
                if(PeerData._ID.equals(Aggregator)){
                    PeerData.com_mtx.release();
                    PeerData.queue.add(new Sextet<>(new ArrayList<>(Collections.singleton(Peer)),Partition,iter,true, null, Hash));
                }
                // If the aggregator downloaded other aggregators date, store them in the Other_Replica_Gradients. This data
                // structure is important in the case where the replica does not respond in the synchronization phase and thus
                // do not lose some amount of gradients.
                else{
                    if(PeerData.Received_Replicas.contains(new Pair<>(Partition,Aggregator))){
                        PeerData.com_mtx.release();
                        // There is no need to aggregate the gradient partition
                        continue;
                    }
                    else if(PeerData.Other_Replica_Gradients.containsKey(new Pair<>(Partition,Aggregator))){
                        PeerData.com_mtx.release();
                        double[] gradients = ipfs.GetParameters(Hash);
                        for(int j = 0; j < gradients.length; j++){
                            PeerData.Other_Replica_Gradients.get(new Pair<>(Partition,Aggregator))[j]  += gradients[j];
                        }
                        gradients = null;
                        PeerData.Other_Replica_Gradients_Received.put(new Pair<>(Partition,Aggregator),PeerData.Other_Replica_Gradients_Received.get(new Pair<>(Partition,Aggregator))+1);

                    }
                    else{
                        PeerData.com_mtx.release();
                        PeerData.Other_Replica_Gradients.put(new Pair<>(Partition,Aggregator),ipfs.GetParameters(Hash));
                        PeerData.Other_Replica_Gradients_Received.put(new Pair<>(Partition,Aggregator),1);

                    }
                }
            }
        }
        for(int i = 0; i < black_list.size(); i++){
            committed_hashes.remove(black_list.get(i));
        }
        black_list = null;

        //System.gc();
        return providers_exist;
    }

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

    // This method is called in order to download the content from the given list. If
    // no provider found for all the gradients return false, else return true
    public boolean download_partial_updates(List<Quartet<String,String,Integer,Integer>> committed_hashes,int iteration) throws Exception{
        Quartet<String,String,Integer,Integer> commitment;
        List<Quartet> black_list = new ArrayList<>();
        boolean providers_exist = false;
        for(int i = 0; i < committed_hashes.size() && ipfs.synch_elapse_time(iteration) > ipfs.get_curr_time() ; i++){
            commitment = committed_hashes.get(i);

            int Partition = commitment.getValue3();
            String Hash = commitment.getValue0();
            String Aggregator = commitment.getValue1();
            int iter = commitment.getValue2();
            if(iter < ipfs.find_iter()){
                return false;
            }
            if(iter != iteration){
                black_list.add(commitment);
                continue;
            }

            int status = download_updates(Hash,Aggregator);
            if((status == 1 || status == 0) && !providers_exist ){
                providers_exist = true;
            }

            if(status == 0 ){
                black_list.add(commitment);

                // newtuple : Replica_ID,partition,iteration,false,gradients
                PeerData.dlog.log("Inserting : " +new ArrayList<>(Collections.singleton(Aggregator)) + " , " + Partition + " , " + iter);
                Sextet<List<String>,Integer,Integer,Boolean,double[],String> newtuple = new Sextet<>(new ArrayList<>(Collections.singleton(Aggregator)),Partition,iter,false, ipfs.Download_Partial_Updates(Hash).getValue1(),null);
                update_participants(Partition,ipfs.Download_Partial_Updates(Hash).getValue0());
                // Because replica sent his aggregated partition, peer doesn't need the gradients he downloaded
                // so he can remove them
                PeerData.com_mtx.acquire();
                if(PeerData.Other_Replica_Gradients.containsKey(new Pair<>(Partition,Aggregator))){
                    PeerData.Other_Replica_Gradients.remove(new Pair<>(Partition,Aggregator));
                    PeerData.Other_Replica_Gradients_Received.remove(new Pair<>(Partition,Aggregator));
                }
                PeerData.Received_Replicas.add(new Pair<>(Partition,Aggregator));
                PeerData.com_mtx.release();

                PeerData.queue.add(newtuple);
            }
            else{
                PeerData.dlog.log("Weird error " + status);
                System.exit(-1);
            }
        }
        for(int i = 0; i < black_list.size(); i++){
            committed_hashes.remove(black_list.get(i));
        }
        black_list = null;
        return providers_exist;
    }

    // This method is called in order to download the content from the given list. If
    // no provider found for all the gradients return false, else return true
    public boolean download_partial_gradients(List<Quintet<String,List<String>,Integer,Integer,String>> merge_responses, Map<Pair<String,Integer>,String> cid_to_peer) throws Exception{
        Quintet<String,List<String>,Integer,Integer,String> response;
        List<Quintet> black_list = new ArrayList<>();
        boolean providers_exist = false;
        for(int i = 0; i < merge_responses.size() && ipfs.aggregation_elapse_time(ipfs.find_iter()) > ipfs.get_curr_time() ; i++){
            response = merge_responses.get(i);
            int Partition = response.getValue3();
            String Hash = response.getValue0();
            List<String> Peer_cids = response.getValue1();
            String Aggregator = response.getValue4();
            //List<Pair<String,Integer>> keySet = new ArrayList<>();
            int iter = response.getValue2();
            int status;
            // Remove the hash of the partial sum
            Peer_cids.remove(0);
            //download the partial sum
            //if(ipfs.find_iter() == iter){
            status = download_updates(Hash,null);
            //}
            if((status == 1 || status == 0) && !providers_exist ){
                providers_exist = true;
            }
            if(status != 0){
                System.out.println("Couldn't download");
            }
            if(status == 0 ){
                black_list.add(response);
                if(Aggregator.equals(PeerData._ID)){
                    PeerData.downloaded_hashes++;

                    PeerData.dlog.log("Gradients downloaded " + Peer_cids + " , P : " + Partition + " , I : " + iter);
                }
                else{
                    PeerData.dlog.log("Other aggregators gradients downloaded " + Aggregator + " , P : " + Partition + " , I : " + iter);
                }

                PeerData.com_mtx.acquire();
                // In case you downloaded the data and the gradients are destined for the aggregator, aggregate the data
                if(PeerData._ID.equals(Aggregator)){
                    PeerData.com_mtx.release();
                    // Map the Cids and their partitions to the corresponding peer
                    PeerData.dlog.log("Cid to peer : " + cid_to_peer +  "  , " + Peer_cids);

                    List<String> Peers = new ArrayList<>();
                    for(i = 0; i < Peer_cids.size(); i++){
                        Peers.add(cid_to_peer.get(new Pair<>(Peer_cids.get(i),Partition)));
                    }
                    // Commit the downloaded data for update
                    PeerData.queue.add(new Sextet<>(Peers,Partition,iter,true,null,Hash));
                    hashes_remaining -= Peer_cids.size();
                }
                else{
                    PeerData.com_mtx.release();
                }

            }
        }
        for(int i = 0; i < black_list.size(); i++){
            merge_responses.remove(black_list.get(i));
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


    // This is the download scheduler for downloading partial updates from peers responsible for the same partition.
    // The main responsibility of this module, is to download the partial updates from aggregators for the same partitions.
    // The downloader might not download all the file of a given update at once (download_updates returns 1), and he might
    // need to call this function more than once, continuing from the point that he timed out.
    public void select_partial_updates() throws Exception{
        List<Quartet<String,String,Integer,Integer>> Committed_hashes = new ArrayList<>();
        int curr_iter = 0;
        List<Quartet> black_list = new ArrayList<>();

        while(true){

            // Wait until you get new commitmets. For efficiency purposes you can download the content of the commitment
            // before aggregation phase starts, but the content is encrypted until the start of the aggregation phase.
            // Also you can't get any commitment in the aggregation phase.


            do {
                if(ipfs.find_iter() == -1){
                    continue;
                }
                if(partial_updates_queue.size() > 0){
                    curr_iter = ipfs.find_iter();
                    PeerData.dlog.log("Found commitments");
                    break;
                }
                if(Committed_hashes.size() > 0){
                    break;
                }
                Thread.yield();
            }while(true);

            // Sleep until training phase elapses
            if(ipfs.training_elapse_time(curr_iter) > ipfs.get_curr_time()){
                Thread.sleep((ipfs.training_elapse_time(curr_iter) - ipfs.get_curr_time())*1000);
            }
            // Get every commitment the peer received
            while(partial_updates_queue.size() != 0){
                Committed_hashes.add(partial_updates_queue.take());
                PeerData.commited_hashes++;

            }

            // If iteration  hasn't finished yet download some content
            if( curr_iter == ipfs.find_iter() && Committed_hashes.size() != 0){
                PeerData.dlog.log("Start downloading partial updates");
                download_partial_updates(Committed_hashes,curr_iter);
                PeerData.dlog.log(Committed_hashes);
            }
            // Otherwise remove all unessecary commitments
            else if(curr_iter != ipfs.find_iter()){
                for(int i = 0; i < Committed_hashes.size(); i++){
                    if(Committed_hashes.get(i).getValue2() < ipfs.find_iter()){
                        black_list.add(Committed_hashes.get(i));
                    }
                }
                for(int i = 0; i  < black_list.size(); i++){
                    Committed_hashes.remove(black_list.get(i));
                }
                black_list = new ArrayList<>();
            }

        }
    }


    public void merge_and_download() throws Exception{
        boolean wait_next_round = true;
        List<Quintet<String,String,Integer,Integer,String>> Committed_hashes = new ArrayList<>();
        List<Quintet<String,List<String>,Integer,Integer,String>> Merged_hashes = new ArrayList<>();
        Map<Pair<String,Integer>,String> Cid_to_participant = new HashMap<>();
        Map<Integer,List<String>> CIDs = new HashMap<>();
        int curr_iter;
        while(true){

            // Wait until you get new commitmets. For efficiency purposes you can download the content of the commitment
            // before aggregation phase starts, but the content is encrypted until the start of the aggregation phase.
            // Also you can't get any commitment in the aggregation phase.
            while(ipfs.find_iter() == -1 || ipfs.training_elapse_time(ipfs.find_iter()) > ipfs.get_curr_time() ){
                if(ipfs.find_iter() != -1){
                    int diff = ipfs.training_elapse_time(ipfs.find_iter()) - ipfs.get_curr_time();
                    if(diff > 0){
                        PeerData.dlog.log("Sleeping for : " + diff*1000);
                        Thread.sleep(diff*1000);
                    }
                    break;
                }
                else{
                    Thread.yield();
                }
            }
            curr_iter = ipfs.find_iter();
            // Get every commitment the peer received
            while(comm_queue.size() != 0){
                Committed_hashes.add(comm_queue.take());
                PeerData.commited_hashes++;
            }

            for(int i = 0; i < PeerData.Auth_List.size(); i++){
                CIDs.put(PeerData.Auth_List.get(i),new ArrayList<>());
            }
            for(int i = 0; i < Committed_hashes.size(); i++){
                Cid_to_participant.put(new Pair<>(Committed_hashes.get(i).getValue0(),Committed_hashes.get(i).getValue3()),Committed_hashes.get(i).getValue1());
                CIDs.get(Committed_hashes.get(i).getValue3()).add(Committed_hashes.get(i).getValue0());
            }
            // Send merge requests to providers and wait to get some responses
            for(int i = 0; i < PeerData.Auth_List.size(); i++){
                Minimize_Gradients_file(CIDs.get(PeerData.Auth_List.get(i)), PeerData.Auth_List.get(i));
            }
            hashes_remaining = PeerData.commited_hashes;
            // Get merge replies and download partial aggregations
            while(ipfs.find_iter() == curr_iter && hashes_remaining != 0 ){
                if(merges_queue.size() == 0 && Merged_hashes.size() == 0){
                    Thread.yield();
                    continue;
                }
                while(merges_queue.size() != 0){
                    Merged_hashes.add(merges_queue.take());
                }
                PeerData.dlog.log(Merged_hashes);
                download_partial_gradients(Merged_hashes,Cid_to_participant);
            }
            Committed_hashes = new ArrayList<>();
            //if(ipfs.synch_elapse_time(ipfs.find_iter()) - ipfs.get_curr_time() > 0){
            //    Thread.sleep((ipfs.synch_elapse_time(ipfs.find_iter()) - ipfs.get_curr_time())*1000);
            //}
        }
    }

    public void add_merge(Quintet merge_response){
        merges_queue.add(merge_response);
    }

    // When a peer receives a commitment checks if it is from his own "client"
    // or others peers client (responsible for the same partition)
    public void add_file(Quintet commitment){
        if(commitment.getValue4().equals(PeerData._ID)){
            comm_queue.add(commitment);
        }
        else{
            Other_Peers_commitments.add(commitment);
        }
    }

    public void add_partial_update(Quartet commitment){
        partial_updates_queue.add(commitment);
    }

    public void cache_partition(String Origin_Peer, int Partition, int Iteration, String Hash) throws Exception{
        Triplet<String, Integer, Integer> pair = new Triplet<>(Origin_Peer, Partition, Iteration);
        ipfs.GetParameters(Hash,PeerData.Weight_Address.get(Partition));
        //for (int i = 0; i < PeerData.Weight_Address.get(Partition).size(); i++) {
        //    PeerData.Weight_Address.get(Partition).set(i, Updated_Partition.get(i));
        //}
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
    }
    public void process_update(List<Quartet<String,String,Integer,Integer>> update) throws Exception{
        List<Integer> existing_partitions = new ArrayList<>();
        for(int i= 0; i < update.size(); i++){

            if(update.get(i).getValue2() < ipfs.find_iter() - 1){
                continue;
            }
            if(!Updates_Collection.containsKey(update.get(i).getValue3()) || Updates_Collection.get(update.get(i).getValue3()).size() > 0 ){
                Updates_Collection.put(update.get(i).getValue3(),new ArrayList<>());
            }

            Updates_Collection.get(update.get(i).getValue3()).add(update.get(i));
            existing_partitions.add(update.get(i).getValue3());
        }
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            if(!existing_partitions.contains(i) && Updates_Collection.containsKey(i)){
                Updates_Collection.remove(i);
            }
        }
    }

    // This method selects the fitting update data for download given the download
    // policy. For example for FIFO policy it returns the first one of all the
    // available updates for the same partition.
    public Quartet<String,String,Integer,Integer> select_provider(int partition){
        if(!Updates_Collection.containsKey(partition) || Updates_Collection.get(partition).size() == 0){
            return null;
        }
        return Updates_Collection.get(partition).get(0);
    }


    public void schedule_updates(List<Integer> Downloaded_partitions, boolean serial) throws Exception{
        Quartet<String,String,Integer,Integer> update;
        Random rn = new Random();
        List<Integer> downloaded = new ArrayList<>();
        int r = rn.nextInt() % PeerData._PARTITIONS;
        if(!serial){
            List<Pair<Integer,String>> Hashes = new ArrayList<>();
            for(int i = 0; i < PeerData._PARTITIONS; i++){
                update = select_provider(i);
                if(update != null || Downloaded_partitions.contains(i)){
                    Hashes.add(new Pair<>(update.getValue3(),update.getValue0()));
                }
            }
            downloaded = ipfs.Batched_download(Hashes);
        }
        //List<Double> Updated_partition = new ArrayList<>();
        for(int i = r; i < PeerData._PARTITIONS + r; i++){
            // If the partition has already by downloaded or exists locally, continue
            //if(PeerData.Auth_List.contains(i) || Downloaded_partitions.contains(i)){
            if(Downloaded_partitions.contains(i%PeerData._PARTITIONS)){
                continue;
            }
            // For each partition, select the hash of its update to download
            update = select_provider(i%PeerData._PARTITIONS);

            // if no hash found continue to next partition
            if(update == null){
                PeerData.dlog.log("Provider didn't found");
                continue;
            }

            int Partition = update.getValue3();
            String Hash = update.getValue0();
            String Peer = update.getValue1();
            int iter = update.getValue2();
            if(PeerData.updates_hashes.contains(Hash)){
                PeerData.downloaded_updates++;
                Downloaded_partitions.add(i%PeerData._PARTITIONS);
                System.out.println("Downloaded Update : " + Partition + " , " + PeerData.Wait_Ack.contains(new Triplet<>(PeerData._ID,Partition,iter)));
                PeerData.Wait_Ack.remove(new Triplet<>(PeerData._ID,Partition,iter));
                PeerData.updates_hashes.remove(Hash);
                continue;
            }
            // Download the content
            int status;
            if(serial){
                status = download_updates(Hash,Peer);
            }
            else{
                if(downloaded.contains(Partition)){
                    status = 0;
                }
                else{
                    status = 1;
                }
            }
            // if the file was fully downloaded then add partition i to the downloaded partitions, cache the
            // partition so that it can be retrieved by GetPartitions
            if(status == 0 || status == 2){
                Downloaded_partitions.add(i%PeerData._PARTITIONS);
                //Updated_partition = ipfs.GetParameters(Hash);
                PeerData.downloaded_updates++;
                if(PeerData.Dealers.containsKey(i%PeerData._PARTITIONS)){
                    cache_partition(PeerData.Dealers.get(i%PeerData._PARTITIONS),Partition,iter,Hash);
                }
                else if(PeerData.Wait_Ack.contains(new Triplet<>("First_Iter",Partition,iter))){
                    cache_partition("First_Iter",Partition,iter,Hash);
                }
                else{
                    String Provider = "";
                    for(int j = 0; j < PeerData.Wait_Ack.size(); j++){
                        if(PeerData.Wait_Ack.get(j).getValue1() == i%PeerData._PARTITIONS){
                            Provider = PeerData.Wait_Ack.get(j).getValue0();
                            break;
                        }
                    }
                    cache_partition(Provider,Partition,iter,Hash);
                }

            }
            else{
                if(status == 1){
                    PeerData.dlog.log("Failed to complete download of : " + Partition);
                }
                //else{
                //    Updates_Collection.get(i).remove(0);
                //    Updates_Collection.get(i).add(update);
                //}
            }
        }
        //System.gc();
        if(downloaded_all_partitions(Downloaded_partitions)){
            Updates_Collection = new HashMap<>();
            Downloaded_partitions = new ArrayList<>();

            //for(int i = 0; i < PeerData._PARTITIONS; i++){
            //    Updates_Collection.put(i,new ArrayList<>());
            //}
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
            if(/*!PeerData.Auth_List.contains(i) && */
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
            if(/*!PeerData.Auth_List.contains(i) && */ !Downloaded_partitions.contains(i) && Updates_Collection.containsKey(i)){
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
        List<Quartet<String,String,Integer,Integer>> update_tuple;
        List<Integer> Downloaded_partitions = new ArrayList<>();

        //for(int i = 0; i < PeerData._PARTITIONS; i++){
        //    Updates_Collection.put(i,new ArrayList<>());
        //}

        while(true){
            while (ipfs.find_iter() == -1){Thread.yield();}
            // In case the update_queue is empty and there aren't hashes of partitions
            // that haven't been downloaded yet then the scheduler must wait.
            if(batched_update_queue.size() == 0 && must_wait(Downloaded_partitions)){
                System.out.println(ANSI_PURPLE + " MUST WAIT " + ANSI_RESET);
                Downloaded_partitions = new ArrayList<>();
                Updates_Collection = new HashMap<>();

                update_tuple = batched_update_queue.take();
                process_update(update_tuple);
            }
            // Get all updates from update_queue
            while(batched_update_queue.size() != 0){
                update_tuple = batched_update_queue.take();
                process_update(update_tuple);
            }
            // If peer didn't download all partitions yet, and he didn't pass the time limits, he can proceed
            // on downloading updated partitions.
            if(!downloaded_all_partitions(Downloaded_partitions) &&
                    ((ipfs.training_elapse_time(ipfs.find_iter()) == -1 ||
                            (ipfs.training_elapse_time(ipfs.find_iter()) != -1 &&
                                    (double)ipfs.get_curr_time() < (double)ipfs.training_elapse_time(ipfs.find_iter()) - (double) ipfs.get_training_time()/6.0)))){
                PeerData.dlog.log("Start downloading");
                schedule_updates(Downloaded_partitions,false);
            }
            else{
                PeerData.dlog.log("Out of time" + (double)ipfs.get_curr_time() + " , " + (double)ipfs.get_training_time()/6.0 + " , " + ((double)ipfs.training_elapse_time(ipfs.find_iter()) - (double) ipfs.get_training_time()/6.0));
                // Do something with unavailable partitions
                Updates_Collection = new HashMap<>();
                Downloaded_partitions = new ArrayList<>();
                //for(int i = 0; i < PeerData._PARTITIONS; i++){
                //    Updates_Collection.put(i,new ArrayList<>());
                //}
                while(batched_update_queue.size() != 0){
                    batched_update_queue.take();
                }
            }
        }
    }

    public void add_update(Quartet<String, String, Integer, Integer> update){
        PeerData.dlog.log("Got new update " + ipfs.get_curr_time());
        update_queue.add(update);
    }

    public void add_batched_updates(List<Quartet<String, String, Integer, Integer>> update){
        PeerData.dlog.log("Got new update " + ipfs.get_curr_time());
        batched_update_queue.add(update);
    }

    public void run() {
        if(mod == 0){
            try {
                if(PeerData.Partial_Aggregation){
                    PeerData.dlog.log("Merge and download is on");
                    merge_and_download();
                }
                else{
                    select_updates();
                }
            }
            catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }

        }
        else if(mod == 1){
            try {select_partial_updates();}
            catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
        }
        else{
            try {get_updated_partitions();}
            catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
