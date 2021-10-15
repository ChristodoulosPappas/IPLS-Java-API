import io.ipfs.api.IPFS;
import io.ipfs.multihash.Multihash;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Triplet;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public void _Update(List<Double> Gradient,int Partiton,String Origin,int iteration,boolean from_clients) throws InterruptedException {
        int i,counter = 0;
        double weight = PeerData.previous_iter_active_workers.get(Partiton);
        //Aggregate gradients from pub/sub
        if(!from_clients) {
            if(PeerData.isSynchronous){
                PeerData.mtx.acquire();

                if(iteration == PeerData.middleware_iteration){
                    System.out.println("From pubsub");
                    for( i = 0; i < PeerData.Aggregated_Gradients.get(Partiton).size() && Gradient != null; i++){
                        PeerData.Replicas_Gradients.get(Partiton).set(i, PeerData.Replicas_Gradients.get(Partiton).get(i) + Gradient.get(i));
                    }
                    PeerData.Replica_Wait_Ack.remove(new Triplet<>(Origin,Partiton,iteration));
                }
                else if(((PeerData.Replica_holders.get(Partiton).contains(Origin) && iteration > PeerData.middleware_iteration) || (PeerData.New_Replicas.get(Partiton).contains(Origin) && iteration > PeerData.middleware_iteration)) && Gradient != null){
                    // Do something for replica holders only. Aggregate in future buffer
                    PeerData.Replica_Wait_Ack_from_future.add(new Triplet<>(Origin,Partiton,iteration));
                }

                PeerData.mtx.release();
            }
            else{
                for (i = 0; i < PeerData.Weights.get(Partiton).size(); i++) {
                    PeerData.Weights.get(Partiton).set(i, 0.75*PeerData.Weights.get(Partiton).get(i) + Gradient.get(i));
                }
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
        else if(from_clients) {
            PeerData.mtx.acquire();



            if(PeerData.isSynchronous){
                // In case a node is so fast sto that he sends the new gradients before the other can finish or if there is a new peer then keep the gradients in a buffer
                // and use them in the next iteration
                if((!PeerData.Client_Wait_Ack.contains(new Triplet<>(Origin,Partiton,iteration)) && PeerData.Clients.get(Partiton).contains(Origin))||(PeerData.New_Clients.get(Partiton).contains(Origin))){
                    System.out.println("Oups , " + iteration + " , " + PeerData.middleware_iteration + " , " + PeerData.Auth_List.contains(Partiton));
                    // !!! This might change
                    if(PeerData.New_Clients.get(Partiton).contains(Origin) && PeerData.middleware_iteration >= iteration){
                        if(!PeerData.workers.get(Partiton).contains(Origin) && Gradient != null) {
                            PeerData.workers.get(Partiton).add(Origin);
                        }
                        for(i = 0; i < PeerData.Weights.get(Partiton).size() && Gradient != null; i++){
                            PeerData.Aggregated_Gradients.get(Partiton).set(i, PeerData.Aggregated_Gradients.get(Partiton).get(i) + Gradient.get(i));
                        }
                    }
                    if(PeerData.middleware_iteration <= iteration){
                        PeerData.Client_Wait_Ack_from_future.add(new Triplet<>(Origin,Partiton,iteration));
                    }
                }
                else if(PeerData.Clients.containsKey(Origin) && PeerData.middleware_iteration < iteration){
                    System.out.println("RECEIVED GRADIENTS FROM FUTURE ? :^)");
                    PeerData.Client_Wait_Ack_from_future.add(new Triplet<>(Origin,Partiton,iteration));
                    if(!PeerData.workers.get(Partiton).contains(Origin) && Gradient != null) {
                        PeerData.workers.get(Partiton).add(Origin);
                    }
                    for(i = 0; i < PeerData.Weights.get(Partiton).size() && Gradient != null; i++){
                        PeerData.Aggregated_Gradients_from_future.get(Partiton).set(i,PeerData.Aggregated_Gradients_from_future.get(Partiton).get(i) + Gradient.get(i));
                    }
                    for(int j = 0; j < PeerData.Client_Wait_Ack.size(); j++){
                        if(PeerData.Client_Wait_Ack.contains(new Triplet<>(Origin,Partiton,PeerData.middleware_iteration))){
                            PeerData.Client_Wait_Ack.remove(new Triplet<>(Origin,Partiton,PeerData.middleware_iteration));
                            break;
                        }
                    }
                }
                else{
                    //System.out.println("Received Gradients");
                    if(!PeerData.workers.get(Partiton).contains(Origin) && Gradient != null) {
                        PeerData.workers.get(Partiton).add(Origin);
                    }
                    for(i = 0; i < PeerData.Weights.get(Partiton).size() && Gradient != null; i++){
                        PeerData.Aggregated_Gradients.get(Partiton).set(i, PeerData.Aggregated_Gradients.get(Partiton).get(i) + Gradient.get(i));
                    }
                    PeerData.Client_Wait_Ack.remove(new Triplet<>(Origin,Partiton,iteration));
                }


            }
            else{
                if(!PeerData.workers.get(Partiton).contains(Origin)) {
                    PeerData.workers.get(Partiton).add(Origin);
                }
                for (i = 0; i < PeerData.Weights.get(Partiton).size(); i++) {
                    PeerData.Weights.get(Partiton).set(i, PeerData.Weights.get(Partiton).get(i) - Gradient.get(i)/weight);
                }

            }
            PeerData.mtx.release();

        }
     }


    public void run(){
        ipfs = new IPFS(PeerData.Path);
        ipfsClass = new MyIPFSClass(PeerData.Path);
        int partition,iteration;
        boolean from_clients;
        List<Double> Gradient;
        String PeerId;
        Quintet<String,Integer,Integer,Boolean, List<Double>> request;
        try {
            while (true) {
                request = PeerData.queue.take();
                partition = request.getValue1();
                Gradient = request.getValue4();
                from_clients = request.getValue3();
                PeerId = request.getValue0();
                iteration = request.getValue2();
                _Update(Gradient, partition,PeerId,iteration,from_clients);

                if(PeerData.isBootsraper){
                    continue;
                }
                if(PeerId != null && PeerId.equals(ipfs.id().get("ID").toString()) == false && !PeerData.isSynchronous) {
                    ipfs.pubsub.pub(PeerId,ipfsClass.Marshall_Packet(PeerData.Weights.get(request.getValue1()),ipfs.id().get("ID").toString(),partition,PeerData.middleware_iteration,(short)4));
                }
                else if(PeerId != null && !PeerData.isSynchronous){
                	for(int i = 0; i < PeerData.Aggregated_Gradients.get(partition).size(); i++) {
                		PeerData.Aggregated_Gradients.get(partition).set(i, 0.25*PeerData.Weights.get(partition).get(i));
                	}
                    if(!PeerData.isSynchronous){
                        ipfs.pubsub.pub(new Integer(partition).toString(),ipfsClass.Marshall_Packet(PeerData.Aggregated_Gradients.get(partition),ipfs.id().get("ID").toString(),PeerData.middleware_iteration,PeerData.workers.size()+1,(short) 3));
                    }
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
