import io.ipfs.api.IPFS;
import io.ipfs.api.Peer;
import io.ipfs.multibase.Base58;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;
import org.nd4j.linalg.api.ops.custom.Tri;
import org.nd4j.linalg.dataset.api.preprocessor.PermuteDataSetPreProcessor;

import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.util.*;

public class IPLS_Comm {
    public static final String TEXT_GREEN = "\u001B[32m";
    public static final String TEXT_RESET = "\u001B[0m";
    MyIPFSClass ipfsClass;
    IPFS ipfs;
    List<Quartet<String,String,Integer,Integer>> commitments = new ArrayList<>();

    public IPLS_Comm(String Path){
        ipfsClass = new MyIPFSClass(Path);
        ipfs = new IPFS(Path);
    }

    public String commit_update( double[] Gradients, int Partition ) throws Exception{
        if(PeerData.Indirect_Communication){
            PeerData.storage_client.sendPartition(PeerData.Provider, PeerData._ID, Partition, PeerData.middleware_iteration,0, Gradients,(byte) 2);
            System.gc();
        }
        if(PeerData.local_save || !PeerData.Indirect_Communication){
            ipfsClass.update_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Updates",Gradients);
            return ipfsClass.add_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Updates").toString();
        }
        return "Nan";
    }

    //public String commit_update( List<Double> Gradients, int Partition) throws Exception{

    //    ipfsClass.Update_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Updates",Gradients);
    //    return ipfsClass.add_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Updates").toString();
    //}

    public String commit_partial_aggregation( List<Double> Gradients, int Partition, String Peer) throws Exception{
        ipfsClass.update_file("IPLS_directory_" + Peer + "/" +  Partition + "_partial_aggregation",Gradients);
        return ipfsClass.add_file("IPLS_directory_" + Peer + "/" +  Partition + "_partial_aggregation").toString();

    }

    public String commit_partial_update( double[] Gradients, int Partition,int workers ) throws Exception{
        if(PeerData.Indirect_Communication){
            PeerData.storage_client.sendPartition(PeerData.Provider, PeerData._ID, Partition, PeerData.middleware_iteration,workers, Gradients,(byte) 1);
            System.gc();
        }
        if(PeerData.local_save ){
            ipfsClass.Update_file("IPLS_directory_" + PeerData._ID + "/" + Partition + "_partial_update", new Pair<>(workers,Gradients));
            return ipfsClass.add_file("IPLS_directory_" + PeerData._ID + "/" + Partition + "_partial_update").toString();
        }
        return "Nan";
    }


    public void commit_partition_update(String Peer, double[] Gradients, int Partition,short pid) throws Exception{
        if(Gradients == null){
            ipfs.pubsub.pub(Peer,ipfsClass.Marshall_Packet("N",PeerData._ID,Partition,PeerData.middleware_iteration,pid));
        }
        else{
            if (PeerData.Indirect_Communication) {
                PeerData.storage_client.sendPartition(PeerData.Provider, Peer, Partition, PeerData.middleware_iteration,0, Gradients,(byte) 0);
                System.gc();
            }
            String Hash = "Nan";
            if(PeerData.local_save){
                ipfsClass.update_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Gradients",Gradients);
                Hash = ipfsClass.add_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Gradients").toString();
            }
            //ipfs.pubsub.pub(Peer,ipfsClass.Marshall_Packet(Hash,PeerData._ID,Partition,PeerData.middleware_iteration,pid));
            commitments.add(new Quartet<>(Peer,Hash,Partition,PeerData.middleware_iteration));
            //PeerData.key_dir.remove(Partition);
            //PeerData.key_dir.put(Partition,new Pair<>(Hash,key));
        }
    }

    public List<Integer> stored_partitions() throws Exception{
        Map<Integer,Triplet<String,String,Integer>> Partition_to_commitment =  new HashMap();
        List<Integer> Uploaded_partitions = new ArrayList<>();
        Map<Integer,String> Hashes_of_Partitions = new HashMap<>();
        for(int i = 0; i < commitments.size(); i++){
            Partition_to_commitment.put(commitments.get(i).getValue2(),new Triplet<>(commitments.get(i).getValue0(),commitments.get(i).getValue1(),commitments.get(i).getValue3()));
        }
        while( ipfsClass.training_elapse_time(PeerData.middleware_iteration)  > ipfsClass.get_curr_time()){
            if(Uploaded_partitions.size() == commitments.size()){
                break;
            }
            else if(PeerData.Partitions_committed.get(0).size() != 0){
                List<Integer> Temp_list = new ArrayList<>();
                PeerData.DS_mtx.acquire();
                for(int i = 0; i < PeerData.Partitions_committed.get(0).size(); i++){
                    if(PeerData.Partitions_committed.get(0).get(i).getValue1() == PeerData.middleware_iteration){
                        Temp_list.add(PeerData.Partitions_committed.get(0).get(i).getValue0());
                        Hashes_of_Partitions.put(PeerData.Partitions_committed.get(0).get(i).getValue0(),PeerData.Partitions_committed.get(0).get(i).getValue2());
                    }
                }
                PeerData.DS_mtx.release();
                for(int i = 0; i < Temp_list.size(); i++){
                    if(!Uploaded_partitions.contains(Temp_list.get(i))){
                        Uploaded_partitions.add(Temp_list.get(i));
                    }
                }
                for(int i = 0 ; i < Temp_list.size(); i++){
                    PeerData.Partitions_committed.get(0).remove(new Triplet<>(Temp_list.get(i),PeerData.middleware_iteration,Hashes_of_Partitions.get(Temp_list.get(i))));
                }
            }

        }
        if(!PeerData.local_save){
            for(int i = 0; i < commitments.size(); i++){
                if(Hashes_of_Partitions.containsKey(commitments.get(i).getValue2())){
                    commitments.set(i,commitments.get(i).setAt1(Hashes_of_Partitions.get(commitments.get(i).getValue2())));
                }
            }
            //System.out.println(commitments);
        }
        return Uploaded_partitions;
    }


    public void publish_commitments(short pid) throws Exception{
        List<byte[]> Aggregators = new ArrayList<>();
        List<byte[]> Hashes = new ArrayList<>();
        List<Integer> Partitions = new ArrayList<>();
        if(PeerData.Indirect_Communication){
            Partitions = stored_partitions();
        }
        else{
            for(int i = 0; i < PeerData._PARTITIONS; i++){
                Partitions.add(i);
            }
        }
        for(int i = 0; i < commitments.size(); i++){
            if(!Partitions.contains(commitments.get(i).getValue2())){
                continue;
            }
            Aggregators.add(Base58.decode(commitments.get(i).getValue0()));
            Hashes.add(Base58.decode(commitments.get(i).getValue1()));
            //Partitions.add(Integer.parseInt(String.valueOf(commitments.get(i).getValue2())));
        }
        PeerData.ds_client.storeGradients(Aggregators,Partitions,Hashes,PeerData.middleware_iteration);
        System.out.println(TEXT_GREEN + "COMMITTED SUCCESFULLY : " + Partitions.size() + " / " + commitments.size() + TEXT_RESET);
        commitments = new ArrayList<>();
    }


    // Write the hashes of the partial updates to the directory service
    public void publish_partial_updates(Map<Integer,String> Partial_Updates){
        List<byte[]> Aggregators = new ArrayList<>();
        List<Integer> Responsibilities = new ArrayList<>(Partial_Updates.keySet());
        for(int i = 0; i < Responsibilities.size(); i++){
            try {
                for(int j = 0; j < PeerData.Replica_holders.get(Responsibilities.get(i)).size(); j++){
                    Aggregators.add(Base58.decode(PeerData.Replica_holders.get(Responsibilities.get(i)).get(j)));
                }
                PeerData.ds_client.storePartial(Responsibilities.get(i),
                        Base58.decode(Partial_Updates.get(Responsibilities.get(i))),
                        Aggregators,
                        PeerData.middleware_iteration);
            }catch (Exception e){
                System.out.println(TEXT_GREEN + "Exception " + e + TEXT_RESET);
            }
        }
    }

    // In case indirect communication is true, then with write_partial_updates we write the
    // hashes of the partial updates that we are sure that stored successfully in the
    // storage network
    public void write_partial_updates(Map<Integer,String> Partial_Updates) throws Exception{
        List<Integer> Uploaded_partitions = new ArrayList<>();
        List<Pair<Integer,String>> Temp_list = new ArrayList<>();
        Map<Integer,String> Acknowledged_Partial_Updates = new HashMap<>();

        // In case Indirect communication is false just writhe the
        // hashes of all partial updates to the DS
        if(!PeerData.Indirect_Communication){
            publish_partial_updates(Partial_Updates);
            return;
        }

        // In case a partition isn't stored successfully in the storage network yet, wait for time
        // sync_elapse_time - 0.2 (elpase_time)

        while(((double)ipfsClass.synch_elapse_time(PeerData.middleware_iteration) - 0.2 * (double) ipfsClass.get_synch_time()) > (double) ipfsClass.get_curr_time()){
            // If all partial updates both written in ds and storage  network, then return
            if(Uploaded_partitions.size() == Partial_Updates.size()){
                System.out.println(TEXT_GREEN + "Uploaded all partial updates" + TEXT_RESET);
                break;
            }
            // otherwise, for each new partition stored in the storage network, write its hash to
            // DS
            else if(PeerData.Partitions_committed.get(1).size() != 0){
                PeerData.DS_mtx.acquire();
                for(int i = 0; i < PeerData.Partitions_committed.get(1).size(); i++){
                    if(PeerData.Partitions_committed.get(1).get(i).getValue1() == PeerData.middleware_iteration){
                        Temp_list.add(new Pair<>(PeerData.Partitions_committed.get(1).get(i).getValue0(),PeerData.Partitions_committed.get(1).get(i).getValue2()));
                    }
                }
                PeerData.DS_mtx.release();
                for(int i = 0; i < Temp_list.size(); i++){
                    if(!Uploaded_partitions.contains(Temp_list.get(i).getValue0())){
                        Uploaded_partitions.add(Temp_list.get(i).getValue0());
                    }
                }
                for(int i = 0 ; i < Temp_list.size(); i++){
                    PeerData.Partitions_committed.get(1).remove(new Triplet<Integer,Integer,String>(Temp_list.get(i).getValue0(),PeerData.middleware_iteration,Temp_list.get(i).getValue1()));
                }
            }
            for(int i = 0; i < Temp_list.size(); i++){
                if(PeerData.local_save){
                    Acknowledged_Partial_Updates.put(Temp_list.get(i).getValue0(),Partial_Updates.get(Temp_list.get(i).getValue0()));
                }
                else{
                    Acknowledged_Partial_Updates.put(Temp_list.get(i).getValue0(),Temp_list.get(i).getValue1());
                }
            }
            // Actually write the new stored partitions in the DS
            if(Acknowledged_Partial_Updates.size() != 0){
                publish_partial_updates(Acknowledged_Partial_Updates);
            }
            Acknowledged_Partial_Updates = new HashMap<>();
            Temp_list = new ArrayList<>();
            Thread.yield();
        }
    }

    public String get_hash(List<Triplet<Integer,Integer,String>> tuples, Pair<Integer,Integer> check_pair) throws Exception{
        PeerData.DS_mtx.acquire();
        for(int i = 0; i < tuples.size(); i++){
            if(tuples.get(i).getValue1().equals(check_pair.getValue1()) &&  tuples.get(i).getValue0().equals(check_pair.getValue0()) ){
                PeerData.DS_mtx.release();
                return tuples.get(i).getValue2();
            }
        }
        PeerData.DS_mtx.release();
        return null;
    }

    // If PeerData.Indirect_Communication is true,the aggregator then upon verifying that his partition
    // was indeed stored in the storage network, he writes to the directory service. If the ACK is received
    // after sync elapse time, then publish_updates returns
    // If Indirect communication is false then the aggregator just writes to the directory service and returns
    public void publish_updates(Map<Integer,String> update_hash) throws Exception{
        String hash;
        if(PeerData.Indirect_Communication){
            int updates_published = 0;
            List<Integer> Responsibilities = new ArrayList<>(update_hash.keySet());
            List<Pair<Integer,String>> black_list = new ArrayList<>();

            while(ipfsClass.synch_elapse_time(PeerData.middleware_iteration) > ipfsClass.get_curr_time()){
                // If written all hashes in directory service, return
                if(Responsibilities.size() == 0){
                    System.out.println(TEXT_GREEN + "Published Updates : " + updates_published + "/" + PeerData.Auth_List.size() + TEXT_RESET);
                    return;
                }

                for(int i = 0; i < Responsibilities.size(); i++){
                    // In case a partition stored in the ds write this partition to the DS
                    hash = get_hash(PeerData.Partitions_committed.get(2),new Pair<Integer,Integer>(new Integer(Responsibilities.get(i)),PeerData.middleware_iteration));
                    if(hash != null){
                        //System.out.println(TEXT_GREEN + "Storing Update : " + Responsibilities.get(i) + TEXT_RESET);
                        black_list.add(new Pair<Integer,String>(Responsibilities.get(i),hash));
                        try {
                            if(PeerData.local_save){
                                PeerData.ds_client.storeUpdate(Integer.parseInt(String.valueOf(Responsibilities.get(i))),
                                        Base58.decode(update_hash.get(Responsibilities.get(i))), new ArrayList<>(), PeerData.middleware_iteration);
                            }
                            else{
                                PeerData.ds_client.storeUpdate(Integer.parseInt(String.valueOf(Responsibilities.get(i))),
                                        Base58.decode(hash), new ArrayList<>(), PeerData.middleware_iteration);
                            }
                            updates_published++;
                        }catch (Exception e){
                            System.out.println(TEXT_GREEN + "Published Updates : " + updates_published + "/" + PeerData.Auth_List.size() + TEXT_RESET);
                            return;
                        }
                    }
                }
                // Remove all partitions stored in the storage network
                for(int i = 0; i < black_list.size(); i++){
                    PeerData.Partitions_committed.get(2).remove(new Triplet<>(black_list.get(i).getValue0(),PeerData.middleware_iteration,black_list.get(i).getValue1()));
                    Responsibilities.remove(new Integer(black_list.get(i).getValue0()));
                }
                black_list = new ArrayList<>();
                Thread.yield();
                try {
                    Thread.sleep(100);
                }catch (Exception e){}
            }

            System.out.println(TEXT_GREEN + "Published Updates : " + updates_published + "/" + PeerData.Auth_List.size() + TEXT_RESET);
        }
        else{
            for(int i = 0; i < PeerData.Auth_List.size(); i++) {
                // Write all partition updates in the DS
                try {
                    PeerData.ds_client.storeUpdate(Integer.parseInt(String.valueOf(PeerData.Auth_List.get(i))),
                            Base58.decode(update_hash.get(PeerData.Auth_List.get(i))), new ArrayList<>(), PeerData.middleware_iteration);
                } catch (Exception e) {
                    System.out.println(TEXT_GREEN + "Unable to commit to the DS the updates" + TEXT_RESET);
                }
            }
        }
    }

    public void send_key(String Peer, String Hash ,int Partition, SecretKey key) throws Exception{
        ipfs.pubsub.pub(String.valueOf(Partition),ipfsClass.Marshall_Packet(PeerData._ID,Hash,Partition,PeerData.middleware_iteration,key,(short)33));
    }

    public void process_commitment(int Partition, String Origin, String Hash,int Iteration, String Aggregator) throws Exception{
        // This is somehow naive and is going to change in the future
        if(ipfsClass.find_iter() == -1 || ( ipfsClass.training_elapse_time(ipfsClass.find_iter()) > ipfsClass.get_curr_time() )){
            if((ipfsClass.find_iter() == -1 && Iteration > PeerData.middleware_iteration ) || ( ipfsClass.find_iter() != -1 && Iteration == ipfsClass.find_iter())){
                //PeerData.Committed_Hashes.get(Partition).add(new Triplet<>(Hash,Origin,Iteration));
                PeerData.aggregation_download_scheduler.add_file(new Quintet(Hash,Origin,Iteration,Partition,Aggregator));
            }
        }
    }
    public void process_commitment(int Partition, List<String> Cids, String Hash,int Iteration, String Aggregator) throws Exception{
        // This is somehow naive and is going to change in the future
        if((ipfsClass.find_iter() == -1 && Iteration > PeerData.middleware_iteration ) || ( ipfsClass.find_iter() != -1 && Iteration == ipfsClass.find_iter())){
            //PeerData.Committed_Hashes.get(Partition).add(new Triplet<>(Hash,Origin,Iteration));
            PeerData.aggregation_download_scheduler.add_merge(new Quintet(Hash,Cids,Iteration,Partition,Aggregator));
        }

    }

}
