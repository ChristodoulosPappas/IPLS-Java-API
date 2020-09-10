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
        if(Origin != null) {
            for (i = 0; i < PeerData.Weights.get(Partiton).size(); i++) {
                PeerData.Weights.get(Partiton).set(i, PeerData.Weights.get(Partiton).get(i) - Gradient.get(i));
                PeerData.Aggregated_Gradients.get(Partiton).set(i, PeerData.Aggregated_Gradients.get(Partiton).get(i) + Gradient.get(i));
            }
        }
        else{
            for (i = 0; i < PeerData.Weights.get(Partiton).size(); i++) {
                PeerData.Weights.get(Partiton).set(i, PeerData.Weights.get(Partiton).get(i) - Gradient.get(i));
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


                if(PeerId != null && PeerId.equals(ipfs.id().get("ID").toString()) == false) {
                    ipfs.pubsub.pub(PeerId,ipfsClass.Marshall_Packet(PeerData.Weights.get(request.getValue1()),ipfs.id().get("ID").toString(),request.getValue1(),(short)4));
                }
                else if(PeerId != null){
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
