import io.ipfs.api.IPFS;
import io.ipfs.multihash.Multihash;
import org.javatuples.*;
import org.nd4j.linalg.indexing.conditions.Or;

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

    public void _Update(double[] Gradient,int Partiton,List<String> Origin,int iteration,boolean from_clients) throws InterruptedException {
        int i,counter = 0;
        //double weight = PeerData.previous_iter_active_workers.get(Partiton);

        //Aggregate gradients from pub/sub
        if(!from_clients) {
            if(PeerData.isSynchronous){
                PeerData.mtx.acquire();

                if(iteration == PeerData.middleware_iteration){
                    PeerData.dlog.log("From pubsub");
                    for( i = 0; i < PeerData.Aggregated_Gradients.get(Partiton).length && Gradient != null; i++){
                        PeerData.Replicas_Gradients.get(Partiton)[i] +=  Gradient[i];
                    }
                    PeerData.Replica_Wait_Ack.remove(new Triplet<>(Origin.get(0),Partiton,iteration));
                }
                else if(((PeerData.Replica_holders.get(Partiton).contains(Origin) && iteration > PeerData.middleware_iteration) || (PeerData.New_Replicas.get(Partiton).contains(Origin) && iteration > PeerData.middleware_iteration)) && Gradient != null){
                    // Do something for replica holders only. Aggregate in future buffer
                    PeerData.Replica_Wait_Ack_from_future.add(new Triplet<>(Origin.get(0),Partiton,iteration));
                }

                PeerData.mtx.release();
            }
            else{
                for (i = 0; i < PeerData.Weights.get(Partiton).length; i++) {
                    PeerData.Weights.get(Partiton)[i] =  0.75*PeerData.Weights.get(Partiton)[i] + Gradient[i];
                }
            }
            return;
        }
        //Aggregate weights from a leaving peer
        else if(Origin.get(0).equals("LeavingPeer")){
            for(i = 0; i < PeerData.Weights.get(Partiton).length; i++){
                PeerData.Weights.get(Partiton)[i] =  a*PeerData.Weights.get(Partiton)[i] + (1-a)*Gradient[i];
            }
        }
        //Aggregate gradients from other peers requests
        else if(from_clients) {
            PeerData.mtx.acquire();

            if(PeerData.isSynchronous && Origin.size() == 1){
                // In case a node is so fast sto that he sends the new gradients before the other can finish or if there is a new peer then keep the gradients in a buffer
                // and use them in the next iteration
                if((!PeerData.Client_Wait_Ack.contains(new Triplet<>(Origin.get(0),Partiton,iteration)) && PeerData.Clients.get(Partiton).contains(Origin.get(0)))||(PeerData.New_Clients.get(Partiton).contains(Origin))){
                    PeerData.dlog.log("Oups , " + iteration + " , " + PeerData.middleware_iteration + " , " + PeerData.Auth_List.contains(Partiton));
                    // !!! This might change
                    if(PeerData.New_Clients.get(Partiton).contains(Origin.get(0)) && PeerData.middleware_iteration >= iteration){
                        if(!PeerData.workers.get(Partiton).contains(Origin.get(0)) && Gradient != null) {
                            PeerData.workers.get(Partiton).add(Origin.get(0));
                        }
                        for(i = 0; i < PeerData.Weights.get(Partiton).length && Gradient != null; i++){
                            PeerData.Aggregated_Gradients.get(Partiton)[i]  = PeerData.Aggregated_Gradients.get(Partiton)[i] + Gradient[i];
                        }
                    }
                    if(PeerData.middleware_iteration <= iteration){
                        PeerData.Client_Wait_Ack_from_future.add(new Triplet<>(Origin.get(0),Partiton,iteration));
                    }
                }
                else if(PeerData.Clients.containsKey(Origin.get(0)) && PeerData.middleware_iteration < iteration){
                    PeerData.dlog.log("RECEIVED GRADIENTS FROM FUTURE ? :^)");
                    PeerData.Client_Wait_Ack_from_future.add(new Triplet<>(Origin.get(0),Partiton,iteration));
                    if(!PeerData.workers.get(Partiton).contains(Origin.get(0)) && Gradient != null) {
                        PeerData.workers.get(Partiton).add(Origin.get(0));
                    }

                    for(i = 0; i < PeerData.Weights.get(Partiton).length && Gradient != null; i++){
                        PeerData.Aggregated_Gradients_from_future.get(Partiton).set(i,PeerData.Aggregated_Gradients_from_future.get(Partiton).get(i) + Gradient[i]);
                    }
                    for(int j = 0; j < PeerData.Client_Wait_Ack.size(); j++){
                        if(PeerData.Client_Wait_Ack.contains(new Triplet<>(Origin.get(0),Partiton,PeerData.middleware_iteration))){
                            PeerData.Client_Wait_Ack.remove(new Triplet<>(Origin.get(0),Partiton,PeerData.middleware_iteration));
                            break;
                        }
                    }

                }
                else{
                    if(!PeerData.workers.get(Partiton).contains(Origin.get(0)) && Gradient != null) {
                        PeerData.workers.get(Partiton).add(Origin.get(0));
                    }

                    for(i = 0; i < PeerData.Weights.get(Partiton).length && Gradient != null; i++){
                        PeerData.Aggregated_Gradients.get(Partiton)[i] =  PeerData.Aggregated_Gradients.get(Partiton)[i] + Gradient[i];
                    }
                    PeerData.Client_Wait_Ack.remove(new Triplet<>(Origin.get(0),Partiton,iteration));

                }
            }
            else if(PeerData.isSynchronous && Origin.size() > 1){
                PeerData.dlog.log(Origin);
                for(i = 0 ; i < Origin.size(); i++) {
                    if (!PeerData.workers.get(Partiton).contains(Origin.get(i)) && Gradient != null) {
                        PeerData.workers.get(Partiton).add(Origin.get(i));
                    }
                }
                for(i = 0; i < PeerData.Weights.get(Partiton).length && Gradient != null; i++){
                    PeerData.Aggregated_Gradients.get(Partiton)[i] = PeerData.Aggregated_Gradients.get(Partiton)[i] + Gradient[i];
                }
                PeerData.dlog.log(PeerData.Client_Wait_Ack);
                for(i = 0; i < Origin.size(); i++){
                    PeerData.Client_Wait_Ack.remove(new Triplet<>(Origin.get(i),Partiton,iteration));
                }
                PeerData.dlog.log(PeerData.Client_Wait_Ack);
            }
            //else{
            //    for(i = 0; i < Origin.size(); i++){
            //        if(!PeerData.workers.get(Partiton).contains(Origin.get(i))) {
            //            PeerData.workers.get(Partiton).add(Origin.get(i));
            //        }
            //    }
            //    for (i = 0; i < PeerData.Weights.get(Partiton).size(); i++) {
            //        PeerData.Weights.get(Partiton).set(i, PeerData.Weights.get(Partiton).get(i) - Gradient.get(i)/weight);
            //    }

            //}
            PeerData.mtx.release();

        }
    }


    public void run(){
        ipfs = new IPFS(PeerData.Path);
        ipfsClass = new MyIPFSClass(PeerData.Path);
        int partition,iteration;
        String Hash;
        boolean from_clients;
        double[] Gradient;
        double[] Gradient_Buff = new double[(int)PeerData._MODEL_SIZE/PeerData._PARTITIONS + 1];
        List<String> PeerId;
        Sextet<List<String>,Integer,Integer,Boolean, double[],String> request;
        for(int i = 0; i < PeerData._MODEL_SIZE/PeerData._PARTITIONS + 1; i++){
            Gradient_Buff[i] = 0.0;
        }
        try {
            while (true) {
                request = PeerData.queue.take();
                partition = request.getValue1();
                Gradient = request.getValue4();
                Hash = request.getValue5();
                // Load the data to Gradient_Buff, in order to save significant amounts
                // of space
                if(Gradient == null && Hash != null){
                    ipfsClass.GetParameters(Hash,Gradient_Buff);
                }
                from_clients = request.getValue3();
                PeerId = request.getValue0();

                iteration = request.getValue2();
                if(Gradient != null){
                    _Update(Gradient, partition,PeerId,iteration,from_clients);
                }
                else{
                    _Update(Gradient_Buff, partition,PeerId,iteration,from_clients);
                }

                if(PeerData.isBootsraper){
                    continue;
                }
                if(PeerId.get(0) != null && PeerId.get(0).equals(ipfs.id().get("ID").toString()) == false && !PeerData.isSynchronous) {
                    ipfs.pubsub.pub(PeerId.get(0),ipfsClass.Marshall_Packet(PeerData.Weights.get(request.getValue1()),ipfs.id().get("ID").toString(),partition,PeerData.middleware_iteration,(short)4));
                }
                else if(PeerId.get(0) != null && !PeerData.isSynchronous){
                    for(int i = 0; i < PeerData.Aggregated_Gradients.get(partition).length; i++) {
                        PeerData.Aggregated_Gradients.get(partition)[i] =  0.25*PeerData.Weights.get(partition)[i];
                    }
                    if(!PeerData.isSynchronous){
                        ipfs.pubsub.pub(new Integer(partition).toString(),ipfsClass.Marshall_Packet(PeerData.Aggregated_Gradients.get(partition),ipfs.id().get("ID").toString(),PeerData.middleware_iteration,PeerData.workers.size()+1,(short) 3));
                    }
                    //Clean Aggregated_Gradients vector
                    for(int i = 0; i < PeerData.Aggregated_Gradients.get(partition).length; i++){
                        PeerData.Aggregated_Gradients.get(partition)[i] = 0.0;
                    }
                }
                request = null;
                Gradient = null;
                //System.gc();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
