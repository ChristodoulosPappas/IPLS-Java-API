import io.ipfs.api.IPFS;
import io.ipfs.api.Peer;
import org.javatuples.*;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

public class SwarmManager extends Thread{
    public static IPFS ipfs;
    public static MyIPFSClass AuxilaryIpfs;
    SwarmManager(){
        ipfs = new IPFS(PeerData.Path);
        AuxilaryIpfs = new MyIPFSClass(PeerData.Path);
    }

    public void remove_from_data_structures(List<String> Crashed){
        //Remove all peers from Existing Peers
        for(int i = 0; i < Crashed.size(); i++){
            PeerData.Existing_peers.remove(Crashed.get(i));
            //Delete from Swarm peer Auth
            PeerData.Swarm_Peer_Auth.remove(Crashed.get(i));
        }

        //Remove from Partition Availability map
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            for(int j = 0; j < Crashed.size(); j++) {
                if (PeerData.Partition_Availability.get(i).contains(Crashed.get(j))) {
                    PeerData.Partition_Availability.get(i).remove(Crashed.get(j));
                }
            }
        }
    }


    public List<String> CrashedPeers() throws Exception {
        int i;
        boolean changed = false;
        List<String> Crashed = new ArrayList<String>();
        List<String> ExistingPeers = new ArrayList<String>();
        List<Peer> Peers = null;
        try{
            Peers = ipfs.swarm.peers();
        }
        catch (Exception e){
            for(i = 0; i < PeerData.Existing_peers.size(); i++){
                Crashed.add(PeerData.Existing_peers.get(i));
            }
            PeerData.Existing_peers.removeAll(Crashed);

            return Crashed;
        }

        //Fill the string list with existing peers
        for(i = 0; i < Peers.size(); i++){
            ExistingPeers.add(Peers.get(i).id.toString());
        }
        //Check peers who don't exist in existing peers
        for(i = 0; i < PeerData.Existing_peers.size(); i++){
            if(!ExistingPeers.contains(PeerData.Existing_peers.get(i))){
                Crashed.add(PeerData.Existing_peers.get(i));
            }
        }

        PeerData.SendMtx.acquire();
        for(i = 0; i < PeerData.Leaving_peers.size(); i++) {
            if(!Crashed.contains(PeerData.Leaving_peers.get(i))) {
                Crashed.add(PeerData.Leaving_peers.get(i));
            }
        }
        PeerData.Leaving_peers.clear();
        PeerData.SendMtx.release();

        remove_from_data_structures(Crashed);

        return Crashed;
    }


    public List<Integer> Find_Gap_Partitions(){
        List<Integer> Partitions = new ArrayList<>();
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            if(PeerData.Auth_List.contains(i) == false && PeerData.Partition_Availability.get(i).size() == 0){
                Partitions.add(i);
            }
        }
        return Partitions;
    }

    public void recover_from_failure(List<Integer> GapPartitions, List<String> Crashed) throws Exception {
        List<Triplet<String,Integer,Integer>> PairsToRemove = new ArrayList<>();
        String Peer;
        Pair<String,Integer> tuple;


        PeerData.SendMtx.acquire();

        if(GapPartitions.size() != 0){
            System.out.println("GAP DETECTED : " + GapPartitions );
            //In case we have concurrent joins
            for(int i = 0; i < GapPartitions.size(); i++){
                PeerData.Auth_List.add(GapPartitions.get(i));
            }
            ipfs.pubsub.pub("Authorities",AuxilaryIpfs.Marshall_Packet(PeerData.Auth_List,null,ipfs.id().get("ID").toString(),(short) 2));
        }
        //In case we are in an update gradient session, we must delete all crashed/disconnected peers
        if (PeerData.sendingGradients) {
            PeerData.mtx.acquire();
            for(int i = 0; i < PeerData.Wait_Ack.size(); i++){
                if(Crashed.contains(PeerData.Wait_Ack.get(i).getValue0())){
                    if(PeerData.Auth_List.contains(PeerData.Wait_Ack.get(i).getValue1())){
                        //put in queue
                        //remove from waitAck
                        //PeerData.queue.add(new Sextet<>(new ArrayList<>(Collections.singleton(ipfs.id().get("ID").toString())),PeerData.Wait_Ack.get(i).getValue1(),PeerData.middleware_iteration,true,PeerData.GradientPartitions.get(PeerData.Wait_Ack.get(i).getValue1()),null));
                        PairsToRemove.add(PeerData.Wait_Ack.get(i));
                    }
                    else{
                        Peer = PeerData.Partition_Availability.get(PeerData.Wait_Ack.get(i).getValue1()).get(0);
                        PeerData.Dealers.put(PeerData.Wait_Ack.get(i).getValue1(),Peer);
                        PeerData.Wait_Ack.add(new org.javatuples.Triplet<>(Peer,PeerData.Wait_Ack.get(i).getValue1(),PeerData.middleware_iteration));
                        PairsToRemove.add(PeerData.Wait_Ack.get(i));
                        //send gradients
                        ipfs.pubsub.pub(Peer,AuxilaryIpfs.Marshall_Packet(PeerData.GradientPartitions.get(PeerData.Wait_Ack.get(i).getValue1()),ipfs.id().get("ID").toString(),PeerData.Wait_Ack.get(i).getValue1(),(short) 3));
                        System.out.println("SEND GRADIENTS : " + PeerData.Wait_Ack.get(i).getValue1());
                    }
                }
            }
            //remove all useless Pairs from Wait_Ack
            for(int i = 0; i < PairsToRemove.size(); i++){
                PeerData.Wait_Ack.remove(PairsToRemove.get(i));
                PeerData.is_alive_counter = 0;
            }
            PeerData.mtx.release();
        }
        PeerData.SendMtx.release();

    }

    void check_replicas() throws InterruptedException {
        int partition;
        String Peer;
        PeerData.mtx.acquire();
        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            partition = PeerData.Auth_List.get(i);
            for(int j = 0; j < PeerData.Replica_holders.get(partition).size(); j++){
                Peer = PeerData.Replica_holders.get(partition).get(j);
                if(!PeerData.Partition_Availability.get(partition).contains(Peer)){
                    PeerData.Replica_holders.get(partition).remove(Peer);
                    PeerData.Replica_Wait_Ack.remove(new Pair<>(Peer,partition));
                }
            }
            //for(int j = 0; j < PeerData.Partition_Availability.get(partition).size(); j++){
            //    Peer = PeerData.Partition_Availability.get(partition).get(j);
            //    if(!PeerData.Replica_holders.get(partition).contains(Peer)){
            //        PeerData.Replica_holders.get(partition).add(Peer);
            //        PeerData.Replica_Wait_Ack.add(new Pair<>(Peer,partition));
            //    }
            //}

        }
        PeerData.mtx.release();
    }


    //Find peers that have been disconnected from the internet and
    // never recovered
    public List<String> Find_Non_Responding_Peers() throws Exception {
        List<Integer> Gap_Partitions = new ArrayList<>();
        List<String> Crashed = new ArrayList<>();

        for(int i = 0; i < PeerData.Wait_Ack.size(); i++){
            Crashed.add(PeerData.Wait_Ack.get(i).getValue0());
            Gap_Partitions.add(PeerData.Wait_Ack.get(i).getValue1());
        }
        remove_from_data_structures(Crashed);
        recover_from_failure(Gap_Partitions,Crashed);

        return null;
    }

    // This is an inactive module. This module checks if peers are crashed, or aborted involuntary. This probably is
    // not going to happen with our systems sematics. However in the near future it is going to be reused and updated.
    public void CRASH_HANDLER() throws Exception{
        int old_clock = PeerData._Iter_Clock;
        List<String> Crashed = new ArrayList<String>();
        List<String> NonRespondingPeers = new ArrayList<>();
        List<Integer> GapPartitions = new ArrayList<>();
        Pair<String,Integer> tuple;
        check_replicas();
        Crashed = CrashedPeers();

        //System.out.println("Swarm manager " + PeerData.Existing_peers);
        //System.out.println(Crashed);
        if(Crashed != null && Crashed.size() != 0) {
            System.out.println("Crashed Peer detected " + Crashed);
            GapPartitions = Find_Gap_Partitions();
            recover_from_failure(GapPartitions,Crashed);
        }
        if(PeerData.Wait_Ack.size() != 0 && old_clock == PeerData._Iter_Clock){
            PeerData.is_alive_counter++;
            if (PeerData.is_alive_counter == 120000) {
                Find_Non_Responding_Peers();
            }
        }
        else{
            old_clock = PeerData._Iter_Clock;
            PeerData.is_alive_counter = 0;
        }
    }

    // In case check_iteration is true that means that the peer has send the data although
    // they never arrived because i was probably sleeping.
    public static boolean Check_iteration(String Peer,int pos) throws  Exception{
        double peer_iteration = ((List<Double>)AuxilaryIpfs.Get_Message(Peer,"Auxiliaries")).get(pos);
        System.out.println( AuxilaryIpfs.Get_Message(Peer,"Auxiliaries"));
        if((int)peer_iteration == PeerData.middleware_iteration){
            return true;
        }
        return false;

    }

    //For each client that i have not yet received gradients, Check if he has uploaded something
    // If for some reason the message never came to my hands, download it and put it for processing
    public static void Check_Gradients() throws Exception{
        String Peer;
        int partition;
        PeerData.mtx.acquire();
        for(int i = 0; i < PeerData.Client_Wait_Ack.size(); i++){
            Peer = PeerData.Client_Wait_Ack.get(i).getValue0();

            partition = PeerData.Client_Wait_Ack.get(i).getValue1();

            if(Check_iteration(Peer,0)){
                Sextet<List<String>,Integer,Integer,Boolean,double[],String> tuple = new Sextet<>(new ArrayList<>(Collections.singleton(Peer)),partition,PeerData.middleware_iteration,true,AuxilaryIpfs.get_Message(Peer,partition+"_Gradients"),null);
                PeerData.queue.add(tuple);

            }
        }
        PeerData.mtx.release();

    }

    public static void update_participants(int partition,int num_of_participants){
        if(!PeerData.Participants.containsKey(partition)){
            PeerData.Participants.put(partition,num_of_participants);
        }
        else{
            PeerData.Participants.replace(partition,PeerData.Participants.get(partition) + num_of_participants);
        }
    }

    public static void Check_Replicas() throws Exception{
        String Peer;
        int partition;
        Map<Integer,Integer> Participants = new HashMap<>();
        PeerData.mtx.acquire();
        for(int i = 0; i < PeerData.Replica_Wait_Ack.size(); i++){
            Peer = PeerData.Replica_Wait_Ack.get(i).getValue0();
            partition = PeerData.Replica_Wait_Ack.get(i).getValue1();

            if(Check_iteration(Peer,1)){
                Sextet<List<String>,Integer,Integer,Boolean,double[],String> tuple = new Sextet(new ArrayList<>(Collections.singleton(Peer)),partition,PeerData.middleware_iteration,false, AuxilaryIpfs.get_Message(Peer,partition+"_Replicas"),null);
                PeerData.queue.add(tuple);
                Participants = AuxilaryIpfs.Get_Participant_Number(Peer,"Participants");
                update_participants(partition,Participants.get(partition));
            }
        }
        PeerData.mtx.release();
    }

    public static void Check_Updates() throws Exception{
        String Peer;
        int partition;
        List<Triplet<String,Integer,Integer>> Black_List = new ArrayList<>();
        PeerData.mtx.acquire();
        for(int i = 0; i < PeerData.Wait_Ack.size(); i++){
            Peer = PeerData.Wait_Ack.get(i).getValue0();
            partition = PeerData.Wait_Ack.get(i).getValue1();

            if(Check_iteration(Peer,2) || PeerData.Wait_Ack.get(i).getValue2() == -1){
                List<Double> Updated_Partition = (List<Double>) AuxilaryIpfs.Get_Message(Peer,partition+"_Updates");
                for(int j = 0; j < PeerData.Weight_Address.get(partition).length; j++){
                    PeerData.Weight_Address.get(partition)[j]  = Updated_Partition.get(j);
                }
                if(PeerData.Servers_Iteration.containsKey(Peer)){
                    if(PeerData.Servers_Iteration.get(Peer) < PeerData.middleware_iteration){
                        PeerData.Servers_Iteration.put(Peer,PeerData.middleware_iteration);
                    }
                }
                else{
                    PeerData.Servers_Iteration.put(Peer,PeerData.middleware_iteration);
                }

                Black_List.add(PeerData.Wait_Ack.get(i));
            }
        }
        for(int i = 0; i < Black_List.size(); i++){
            PeerData.Wait_Ack.remove(Black_List.get(i));
        }
        Black_List = null;
        PeerData.mtx.release();

    }

    public static void CHECK_INDIRECT_REQUESTS() throws Exception{
        if(PeerData.Client_Wait_Ack.size() != 0 ){
            Check_Gradients();
        }
        if(PeerData.Replica_Wait_Ack.size() != 0 ){
            Check_Replicas();
        }
        if(PeerData.Wait_Ack.size() != 0){
            Check_Updates();
        }

    }

    public int IPLS_Peer_Scheduler() throws Exception {
        int sleep_time = 1;
        PeerData.mtx.acquire();
        if(PeerData.Schedule_Hash == null || AuxilaryIpfs.find_iter() == -1){
            sleep_time = 1;
        }

        if(AuxilaryIpfs.get_curr_time() < AuxilaryIpfs.training_elapse_time(AuxilaryIpfs.find_iter())){
            sleep_time = AuxilaryIpfs.aggregation_elapse_time(PeerData.middleware_iteration) - AuxilaryIpfs.get_curr_time();
        }
        else if(AuxilaryIpfs.get_curr_time() < AuxilaryIpfs.aggregation_elapse_time(AuxilaryIpfs.find_iter())){
            sleep_time = AuxilaryIpfs.aggregation_elapse_time(AuxilaryIpfs.find_iter()) - AuxilaryIpfs.get_curr_time();
        }
        else if(AuxilaryIpfs.get_curr_time() < AuxilaryIpfs.synch_elapse_time(AuxilaryIpfs.find_iter())){
            AuxilaryIpfs.clear_client_wait_ack_list();
            sleep_time = AuxilaryIpfs.synch_elapse_time(AuxilaryIpfs.find_iter()) - AuxilaryIpfs.get_curr_time();
        }
        else if(AuxilaryIpfs.get_curr_time() > AuxilaryIpfs.synch_elapse_time(AuxilaryIpfs.find_iter())){
            //CLEAR REPLICAS WAITING TABLE
            AuxilaryIpfs.clear_replica_wait_ack_list();
            if(AuxilaryIpfs.training_elapse_time(AuxilaryIpfs.find_iter() + 1) != -1 &&
                    AuxilaryIpfs.get_curr_time() >= AuxilaryIpfs.training_elapse_time(AuxilaryIpfs.find_iter()+1) - AuxilaryIpfs.get_training_time()/6){
                AuxilaryIpfs.clear_wait_ack_list();
                sleep_time = AuxilaryIpfs.training_elapse_time(AuxilaryIpfs.find_iter()+1) - AuxilaryIpfs.get_curr_time();
            }
            /*
            else if(AuxilaryIpfs.training_elapse_time(PeerData.middleware_iteration + 1) != -1 &&
                    AuxilaryIpfs.get_curr_time() >= AuxilaryIpfs.training_elapse_time(PeerData.middleware_iteration+1) &&
                    AuxilaryIpfs.find_iter() != -1){
                System.out.println("Peer Out of sync!");
                PeerData.middleware_iteration = AuxilaryIpfs.find_iter();
            }
            */
            else{
                sleep_time = 2;
            }
        }
        //PeerData.middleware_iteration = find_iter();
        PeerData.mtx.release();

        return sleep_time + 10;
    }

    public void run(){
        String Peer;
        int iteration;
        int sleep_time;
        while(true){
            try {

                if(PeerData.Relaxed_SGD){
                    sleep_time = IPLS_Peer_Scheduler();
                    if(sleep_time>0){
                        //Thread.sleep(sleep_time*1000);
                        Thread.sleep(10*1000);
                    }
                }
                else {
                    iteration = PeerData.middleware_iteration;
                    Thread.sleep(20000);
                    if (iteration == PeerData.middleware_iteration && PeerData.IPNS_Enable) {
                        System.out.println("Checking Indirect requests");
                        CHECK_INDIRECT_REQUESTS();
                    }
                }

            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
