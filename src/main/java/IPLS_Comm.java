import io.ipfs.api.IPFS;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;

import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;

public class IPLS_Comm {
    MyIPFSClass ipfsClass;
    IPFS ipfs;
    List<Quartet<String,String,Integer,Integer>> commitments = new ArrayList<>();

    public IPLS_Comm(String Path){
        ipfsClass = new MyIPFSClass(Path);
        ipfs = new IPFS(Path);
    }

    public String commit_update( List<Double> Gradients, int Partition) throws Exception{
        ipfsClass.Update_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Updates",Gradients);
        return ipfsClass.add_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Updates").toString();
    }

    public void commit_partition_update(String Peer, List<Double> Gradients, int Partition,short pid) throws Exception{
        if(Gradients == null){
            ipfs.pubsub.pub(Peer,ipfsClass.Marshall_Packet("N",PeerData._ID,Partition,PeerData.middleware_iteration,pid));
        }
        else{
            SecretKey key = ipfsClass.Generate_key();
            ipfsClass.Update_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Gradients",Gradients,key);
            String Hash = ipfsClass.add_file("IPLS_directory_" + PeerData._ID + "/" +  Partition + "_Gradients").toString();
            //ipfs.pubsub.pub(Peer,ipfsClass.Marshall_Packet(Hash,PeerData._ID,Partition,PeerData.middleware_iteration,pid));
            commitments.add(new Quartet<>(Peer,Hash,Partition,PeerData.middleware_iteration));
            PeerData.key_dir.remove(Partition);
            PeerData.key_dir.put(Partition,new Pair<>(Hash,key));
        }
    }

    public void publish_commitments(short pid) throws Exception{
        for(int i = 0; i < commitments.size(); i++){
            ipfs.pubsub.pub(String.valueOf(commitments.get(i).getValue2()),ipfsClass.Marshall_Packet(commitments.get(i).getValue1(),PeerData._ID,commitments.get(i).getValue0(),commitments.get(i).getValue2(),commitments.get(i).getValue3(),pid));
        }
        commitments = new ArrayList<>();
    }

    public void send_key(String Peer, String Hash ,int Partition, SecretKey key) throws Exception{
        ipfs.pubsub.pub(String.valueOf(Partition),ipfsClass.Marshall_Packet(PeerData._ID,Hash,Partition,PeerData.middleware_iteration,key,(short)33));
    }

    public void process_commitment(int Partition, String Origin, String Hash,int Iteration, String Aggregator) throws Exception{
        // This is somehow naive and is going to change in the future
        if(ipfsClass.find_iter() == -1 || (ipfsClass.find_iter() != -1 && ipfsClass.training_elapse_time(ipfsClass.find_iter()) > ipfsClass.get_curr_time() )){
            if((ipfsClass.find_iter() == -1 && Iteration > PeerData.middleware_iteration ) || ( ipfsClass.find_iter() != -1 && Iteration == ipfsClass.find_iter())){
                //System.out.println("Got hash : " + Hash + " ,from : " + Origin + " , " + Iteration + " , "  + PeerData.middleware_iteration);
                //PeerData.Committed_Hashes.get(Partition).add(new Triplet<>(Hash,Origin,Iteration));
                PeerData.aggregation_download_scheduler.add_file(new Quintet(Hash,Origin,Iteration,Partition,Aggregator));
            }
        }
        else{
        //    System.out.println("Commitment did not accepted");
        }

    }



}
