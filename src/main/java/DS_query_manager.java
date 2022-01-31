import io.ipfs.api.IPFS;
import io.ipfs.api.Pair;
import io.ipfs.multibase.Base58;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;

import java.util.*;



public class DS_query_manager extends Thread{
    int mod;
    IPFS ipfs;
    IPLS_Comm commit;
    MyIPFSClass ipfsClass;

    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_RESET = "\u001B[0m";
    //If mod is 0, then the query manager asks for gradients
    //else if mod is 1, then the query manager asks for partial updates
    public DS_query_manager(int mod){
        this.mod = mod;
        ipfs = new IPFS(PeerData.Path);
        ipfsClass = new MyIPFSClass(PeerData.Path);
    }

    public List<Triplet> remove_dropouts(List<org.javatuples.Pair<String,Integer>> committed_gradients, int curr_iter) throws Exception{
        List<Triplet<String,Integer,Integer>> ack_list = new ArrayList<>();
        List<Triplet> black_list = new ArrayList<>();
        boolean dropout = true;
        PeerData.mtx.acquire();
        for(int i= 0 ; i < PeerData.Client_Wait_Ack.size(); i++){
            ack_list.add(PeerData.Client_Wait_Ack.get(i));
        }
        PeerData.mtx.release();
        for(int i = 0; i < ack_list.size(); i++){
            for(int j = 0; j < committed_gradients.size(); j++){
                if(ack_list.get(i).getValue0().equals(committed_gradients.get(j).getValue0()) &&
                        ack_list.get(i).getValue1() == (int)committed_gradients.get(j).getValue1() &&
                        curr_iter == ack_list.get(i).getValue2()){
                    dropout = false;
                    break;
                }
            }
            if(dropout){
                black_list.add(ack_list.get(i));
            }
            dropout = true;
        }
        return black_list;
    }

    public void poll_for_gradient_partitions() throws Exception{
        int curr_iter = 0;
        int print_counter = 0;
        boolean print_flag = false;
        List<Triplet> black_list = new ArrayList<>();
        List<org.javatuples.Pair<String,Integer>> committed_gradients = new ArrayList<>();

        Map<Integer, List<Pair<byte[], byte[]>>> gradient_commitments = new HashMap<>();
        while(true){
            while (PeerData.Auth_List.size() == 0){
                Thread.sleep(500);
            }
            while (ipfsClass.find_iter() == -1 ){
                Thread.yield();
            }
            curr_iter = ipfsClass.find_iter();
            // Download the gradient commitments that didn't downloaded yet
            try {
                gradient_commitments = PeerData.ds_client.getGradients(ipfs, new HashSet<>(PeerData.Auth_List), curr_iter);
            }catch (Exception e){
                gradient_commitments = null;
            }
            // Pass those commitments to the download scheduler
            for(int i = 0; i < PeerData.Auth_List.size(); i++){
                if(gradient_commitments == null || gradient_commitments.get(PeerData.Auth_List.get(i)) == null){
                    continue;
                }
                for (Pair<byte[], byte[]> p : gradient_commitments.get(PeerData.Auth_List.get(i))) {
                    PeerData.aggregation_download_scheduler.add_file(new Quintet(Base58.encode(p.right),new String(p.left),curr_iter,PeerData.Auth_List.get(i),PeerData._ID));
                    committed_gradients.add(new org.javatuples.Pair<>(new String(p.left),PeerData.Auth_List.get(i)));
                    print_flag = true;
                }
            }
            if( print_flag || print_counter%4 == 0){
                System.out.println(ANSI_BLUE + "Poll server" + gradient_commitments + " . " + ANSI_RESET);
            }
            print_counter++;
            print_flag = false;
            // In case training time elapsed, and downloaded all the hashes, then wait until the next round
            if(ipfsClass.get_curr_time() > ipfsClass.training_elapse_time(curr_iter) && ipfsClass.get_curr_time() < ipfsClass.synch_elapse_time(curr_iter)){
                black_list = remove_dropouts(committed_gradients,curr_iter);
                for(int i = 0; i < black_list.size(); i++){
                    PeerData.Client_Wait_Ack.remove(black_list.get(i));
                }
                black_list = new ArrayList<>();
                committed_gradients = new ArrayList<>();
                System.out.println(ANSI_BLUE +"Going to sleep"+ ANSI_RESET);
                while(ipfsClass.find_iter() == curr_iter ){
                    Thread.yield();
                }
            }
            Thread.sleep(1000);
        }
    }

    public void poll_for_partial_updates() throws Exception{
        int curr_iter = 0;
        int print_counter = 0;
        boolean print_flag = false;
        Map<Integer, List<Pair<byte[], byte[]>>> partial_sum_commitments = new HashMap<>();

        while(true){
            while (PeerData.Auth_List.size() == 0){
                Thread.sleep(500);
            }
            while (ipfsClass.find_iter() == -1){
                Thread.yield();
            }
            curr_iter = ipfsClass.find_iter();
            if(ipfsClass.training_elapse_time(curr_iter) > ipfsClass.get_curr_time()){
                System.out.println(ANSI_RED + "Going to sleep" + ANSI_RESET);
                PeerData.dlog.log(ANSI_RED + "Sleeping for : " + new Integer(ipfsClass.training_elapse_time(curr_iter)- ipfsClass.get_curr_time()) + ANSI_RESET);
                Thread.sleep((ipfsClass.training_elapse_time(curr_iter)- ipfsClass.get_curr_time())*1000);
            }
            if(PeerData.Auth_List.size() == 0){
                System.out.println(ANSI_RED + "Empty auth list : " + PeerData.Auth_List + ANSI_RESET);
                Thread.sleep(1000);
                continue;
            }
            // Download the gradient commitments that didn't downloaded yet
            try {
                partial_sum_commitments = PeerData.ds_client.getPartials(new HashSet<>(PeerData.Auth_List),curr_iter);
                // Pass those commitments to the download scheduler
            }catch (Exception e){
                System.out.println(ANSI_RED + "Exception " + e + ANSI_RESET);
            }

            for(int i = 0; i < PeerData.Auth_List.size(); i++){
                if(partial_sum_commitments == null || partial_sum_commitments.get(PeerData.Auth_List.get(i)) == null){
                    continue;
                }
                for (Pair<byte[], byte[]> p : partial_sum_commitments.get(PeerData.Auth_List.get(i))) {
                    // String : Hash, String : Aggregator, int : iteration , int partition
                    PeerData.partial_updates_download_scheduler.add_partial_update(new Quartet(Base58.encode(p.right),new String(p.left),curr_iter,PeerData.Auth_List.get(i)));
                    print_flag = true;
                }
            }
            if(print_flag  || print_counter % 4 == 0){
                System.out.println(ANSI_RED + "Poll server, answer : " + partial_sum_commitments + " " + ANSI_RESET);
            }
            print_flag = false;
            print_counter++;
            Thread.sleep(1000);
        }
    }
    public void run() {
        if(mod == 0){
            try { poll_for_gradient_partitions();} catch (Exception e){e.printStackTrace();}
        }
        else{
            try{poll_for_partial_updates();} catch (Exception e){e.printStackTrace();}
        }
    }
}
