import io.ipfs.api.IPFS;
import io.ipfs.api.Peer;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SwarmManager extends Thread{
    IPFS ipfs;
    MyIPFSClass AuxilaryIpfs;
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
        System.out.println(ExistingPeers);
        //Check peers who don't exist in existing peers
        for(i = 0; i < PeerData.Existing_peers.size(); i++){
            if(!ExistingPeers.contains(PeerData.Existing_peers.get(i))){
                Crashed.add(PeerData.Existing_peers.get(i));
            }
        }
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
        List<Pair<String,Integer>> PairsToRemove = new ArrayList<>();
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
                        PeerData.queue.add(new Triplet<String, Integer, List<Double>>(ipfs.id().get("ID").toString(),PeerData.Wait_Ack.get(i).getValue1(),PeerData.GradientPartitions.get(PeerData.Wait_Ack.get(i).getValue1())));
                        PairsToRemove.add(PeerData.Wait_Ack.get(i));
                    }
                    else{
                        Peer = PeerData.Partition_Availability.get(PeerData.Wait_Ack.get(i).getValue1()).get(0);
                        tuple = new org.javatuples.Pair<>(Peer,PeerData.Wait_Ack.get(i).getValue1());
                        PeerData.Wait_Ack.add(tuple);
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

    public void run(){
        List<String> Crashed = new ArrayList<String>();
        List<String> NonRespondingPeers = new ArrayList<>();
        List<Integer> GapPartitions = new ArrayList<>();
        Pair<String,Integer> tuple;
        String Peer;
        while(true){
            try {
                Thread.sleep(400);
                Crashed = CrashedPeers();
                System.out.println("Swarm manager " + PeerData.Existing_peers);
                System.out.println(Crashed);
                if(Crashed != null && Crashed.size() != 0) {
                    System.out.println("Crashed Peer detected " + Crashed);
                    GapPartitions = Find_Gap_Partitions();
                    recover_from_failure(GapPartitions,Crashed);
                }
                if(PeerData.Wait_Ack.size() != 0){
                    PeerData.is_alive_counter++;
                    if (PeerData.is_alive_counter == 1200) {
                        Find_Non_Responding_Peers();
                    }
                }
                else{
                    PeerData.is_alive_counter = 0;
                }

                //if(PeerData.Swarm_Peer_Auth.size() != 0){
                //    System.out.println(PeerData.Swarm_Peer_Auth);
                //}
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
