import io.ipfs.api.IPFS;
import io.ipfs.multihash.Multihash;
import org.javatuples.Triplet;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class Updater extends Thread{
    IPFS ipfs;
    MyIPFSClass ipfsClass;
    double a = 0.6;


    public Multihash _Upload_File(List<Double> Weights, MyIPFSClass ipfsClass, String filename) throws IOException {
        //Serialize the Partition model into a file
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(Weights);
        oos.close();
        fos.close();
        // Add file into ipfs system
        return  ipfsClass.add_file(filename);
    }

    public void _Update(List<Double> Gradient,int Partiton,String Origin){
        int i,counter = 0;
        double weight = PeerData.previous_iter_active_workers.get(Partiton);
        //Aggregate gradients from pub/sub
        if(Origin == null) {
            System.out.println("From pubsub");
            for (i = 0; i < PeerData.Weights.get(Partiton).size(); i++) {
                PeerData.Weights.get(Partiton).set(i, 0.75*PeerData.Weights.get(Partiton).get(i) + Gradient.get(i));
            }
            return;
        }
        //System.out.println(Origin + " , " + Origin==null + " , " + Origin.equals(null));
        //Aggregate weights from a leaving peer
        else if(Origin.equals("LeavingPeer")){
            for(i = 0; i < PeerData.Weights.get(Partiton).size(); i++){
                PeerData.Weights.get(Partiton).set(i, a*PeerData.Weights.get(Partiton).get(i) + (1-a)*Gradient.get(i));
            }
        }
        //Aggregate gradients from other peers requests
        else if(Origin != null) {
        	System.out.println(1/weight);
            for (i = 0; i < PeerData.Weights.get(Partiton).size(); i++) {
                PeerData.Weights.get(Partiton).set(i, PeerData.Weights.get(Partiton).get(i) - Gradient.get(i)/weight);
                PeerData.Aggregated_Gradients.get(Partiton).set(i, PeerData.Aggregated_Gradients.get(Partiton).get(i) + Gradient.get(i));
            }
        }
     }


    public void run(){
        ipfs = new IPFS(PeerData.Path);
        ipfsClass = new MyIPFSClass(PeerData.Path);
        int partition;
        List<Double> Gradient;
        String PeerId,reply;
        Triplet<String,Integer, List<Double>> request;
        Multihash hash;
        try {
            while (true) {
                request = PeerData.queue.take();
                partition = request.getValue1();
                Gradient = request.getValue2();
                PeerId = request.getValue0();

                _Update(Gradient, partition,PeerId);

                if(PeerData.isBootsraper){
                    continue;
                }
                if(PeerId != null && PeerId.equals(ipfs.id().get("ID").toString()) == false) {
                    ipfs.pubsub.pub(PeerId,ipfsClass.Marshall_Packet(PeerData.Weights.get(request.getValue1()),ipfs.id().get("ID").toString(),partition,(short)4));
                }
                else if(PeerId != null){
                	for(int i = 0; i < PeerData.Aggregated_Gradients.get(partition).size(); i++) {
                		PeerData.Aggregated_Gradients.get(partition).set(i, 0.25*PeerData.Weights.get(partition).get(i));
                	}
                    ipfs.pubsub.pub(new Integer(partition).toString(),ipfsClass.Marshall_Packet(PeerData.Aggregated_Gradients.get(partition),ipfs.id().get("ID").toString(),partition,(short) 3));
                    //Clean Aggregated_Gradients vector
                    for(int i = 0; i < PeerData.Aggregated_Gradients.get(partition).size(); i++){
                        PeerData.Aggregated_Gradients.get(partition).set(i,0.0);
                    }
                }

            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
