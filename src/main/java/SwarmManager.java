import io.ipfs.api.IPFS;
import io.ipfs.api.Peer;

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
        for(i = 0; i < PeerData.peers.size(); i++){
            if(ExistingPeers.contains(PeerData.Existing_peers.get(i)) == false){
                Crashed.add(PeerData.Existing_peers.get(i));
            }
        }
        //Add in global ExistingPeers the new peers and remove crashed peers
        PeerData.Existing_peers.removeAll(Crashed);
        for(i = 0; i < ExistingPeers.size(); i++){
            if(PeerData.Existing_peers.contains(ExistingPeers.get(i)) == false){
                PeerData.Existing_peers.add(ExistingPeers.get(i));
                changed = true;
            }
        }
        if(changed){
            ipfs.pubsub.pub("Authorities", AuxilaryIpfs.Marshall_Packet(PeerData.Auth_List,null, ipfs.id().get("ID").toString(),(short) 2));
        }
        //
        //return chrashed peers
        return Crashed;
    }

    //Remove Data from Crashed peers
    public void UpdatePeerData(List<String> Crashed){
        int i,j;
        List<Integer> Partitions = new ArrayList<Integer>();
        //For every peer get Partitions, and remove them from PartitionAvailability, and also SwarmPeerAuth
        for(i = 0; i < Crashed.size(); i++){
            Partitions = PeerData.Swarm_Peer_Auth.get(Crashed.get(i));
            PeerData.Swarm_Peer_Auth.remove(Crashed.get(i));
            for(j = 0; j < Partitions.size(); j++){
                PeerData.Partition_Availability.get(Partitions.get(i)).remove(Crashed.get(i));

            }
        }
    }

    public void run(){
        List<String> Crashed = new ArrayList<String>();
        while(true){
            try {
                Thread.sleep(400);
                Crashed = CrashedPeers();
                if(Crashed != null && Crashed.size() != 0) {
                    System.out.println("Crashed Peer detected " + Crashed);
                    UpdatePeerData(Crashed);
                    if (PeerData.sendingGradients) {
                        //resend Gradient Parts in case you send them to a crashed peer and have not
                        // received ACK

                    }
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
