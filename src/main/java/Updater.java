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
    MyIPFSClass AuxilaryIpfs;



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

    public void _Update(List<Double> Gradient,int Partiton){
        int i,counter = 0;
        List<Double> PWeight = new ArrayList<>();


        for(i = 0; i < PeerData.Weights.get(Partiton).size(); i++){
            PWeight.add(PeerData.Weights.get(Partiton).get(i) - Gradient.get(i));
        }

        PeerData.Weights.replace(Partiton,PWeight);

    }


    public void run(){
        ipfs = new IPFS(PeerData.Path);
        String PeerId,reply;
        Triplet<String,Integer, List<Double>> request;
        Multihash hash;
        try {
            while (true) {
                request = PeerData.queue.take();
                _Update(request.getValue2(), request.getValue1());
                PeerId = request.getValue0();
                //hash = _Upload_File(PeerData.Weights.get(request.getValue1()),AuxilaryIpfs,"MFP" + request.getValue1());
                //PeerData.LastUpdate.put(request.getValue1(),hash);
                if(PeerId.equals(ipfs.id().get("ID").toString()) == false) {
                    ipfs.pubsub.pub(PeerId,AuxilaryIpfs.Marshall_Packet(PeerData.Weights.get(request.getValue1()),ipfs.id().get("ID").toString(),request.getValue1(),(short)4));
                }

            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
