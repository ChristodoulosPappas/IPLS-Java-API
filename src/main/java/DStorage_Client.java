
import io.ipfs.api.IPFS;
import io.ipfs.api.Sub;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.web3j.protocol.core.methods.response.EthLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public class DStorage_Client extends Thread{
    IPFS ipfs;
    MyIPFSClass ipfsClass;
    private final BlockingQueue<List<Double>> partitionsQueue  = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> ipfsNodesQueue = new LinkedBlockingQueue<>();
    private final String id; // for convenience

    public DStorage_Client() throws IOException {
        ipfs = new IPFS(PeerData.Path);
        ipfsClass = new MyIPFSClass(PeerData.Path);
        id = (String) ipfs.id().get("ID");

    }

    /* Discover an IPFS node by using the dedicated discovery topic */
    public String getIPFSNode() throws Exception {
        /* clear the queue from the previous call */
        ipfsNodesQueue.clear();

        ByteBuffer buf = ByteBuffer.allocate(id.length() + 1);
        buf.put(Constants.MessageTags.Discover);
        buf.put(id.getBytes());
        ipfs.pubsub.pub(Constants.Discovery.Topic + "Storage", Base64.getUrlEncoder().encodeToString(buf.array()));
        /* Wait for ipfs nodes to reply
        Another thread handles these and puts their IDs to ipfsNodesQueue */
        return ipfsNodesQueue.take();
    }

    /* Discover an IPFS node by using the dedicated discovery topic */
    public String[] get_Storage_View() throws Exception {
        /* clear the queue from the previous call */
        ipfsNodesQueue.clear();

        ByteBuffer buf = ByteBuffer.allocate(id.length() + 1);
        buf.put(Constants.MessageTags.Discover);
        buf.put(id.getBytes());
        ipfs.pubsub.pub(Constants.Discovery.Topic + "Storage", Base64.getUrlEncoder().encodeToString(buf.array()));
        System.out.println("Send query");
        Thread.sleep(5000);
        String[] Storage_nodes_id = new String[ipfsNodesQueue.size()];
        for(int i = 0; i < Storage_nodes_id.length; i++){
            Storage_nodes_id[i] = ipfsNodesQueue.take();
        }
        System.out.print("Received " + Storage_nodes_id.length);
        /* Wait for ipfs nodes to reply
        Another thread handles these and puts their IDs to ipfsNodesQueue */
        return Storage_nodes_id;
    }

    public void sendPartition(String ipfsNodeID, String aggregator, int partition, int iteration,int workers,
                              List<Double> data, byte mod) throws Exception {

        ByteBuffer buf = ByteBuffer.allocate(Double.BYTES * data.size()
                + 6 * Integer.BYTES
                + id.length()
                + aggregator.length()
                + 2);
        buf.put(Constants.MessageTags.SendPartition);
        buf.put(mod);
        buf.putInt(id.length());
        buf.put(id.getBytes());
        buf.putInt(aggregator.length());
        buf.put(aggregator.getBytes());
        buf.putInt(partition);
        buf.putInt(iteration);
        buf.putInt(workers);
        buf.putInt(data.size());
        for(double d : data)
            buf.putDouble(d);

        ipfs.pubsub.pub(ipfsNodeID + "Storage", Base64.getUrlEncoder().encodeToString(buf.array()));
    }

    public void sendPartition_all(String ipfsNodeID, String aggregator, int partition, int iteration,int workers,
                              double[] data, byte mod) throws Exception{
        String[] Replication_Nodes = new String[3];
        int size = 1;
        if(PeerData.Gradients_Replication){
            int j;
            for(int i = 0; i < 3; i++){
                j = ThreadLocalRandom.current().nextInt(0, PeerData.View.length + 1);
                size+= Integer.BYTES + PeerData.View[j].length();
                Replication_Nodes[j] = PeerData.View[j];
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(Double.BYTES * data.length
                + 6 * Integer.BYTES
                + id.length()
                + aggregator.length()
                + 2 + size);
        buf.put(Constants.MessageTags.SendPartition);
        buf.put(mod);
        buf.putInt(id.length());
        buf.put(id.getBytes());
        buf.putInt(aggregator.length());
        buf.put(aggregator.getBytes());
        buf.putInt(partition);
        buf.putInt(iteration);
        buf.putInt(workers);
        buf.putInt(data.length);
        if(PeerData.Gradients_Replication){
            buf.put((byte) 1);
            for(int i = 0; i < 3; i++) {
                buf.putInt(Replication_Nodes[i].length());
                buf.put(Replication_Nodes[i].getBytes());
            }
        }
        else{
            buf.put((byte) 0);
        }

        for(int i =0; i < data.length; i++){
            buf.putDouble(data[i]);
        }

        ipfs.pubsub.pub(ipfsNodeID + "Storage", Base64.getUrlEncoder().encodeToString(buf.array()));
        buf = null;

    }

    public void sendPartition(String ipfsNodeID, String aggregator, int partition, int iteration,int workers,
                              double[] data, byte mod) throws Exception {

        String[] Replication_Nodes = new String[3];
        int size = 1;
        if(PeerData.Gradients_Replication){
            int j;
            for(int i = 0; i < 3; i++){
                j = ThreadLocalRandom.current().nextInt(0, PeerData.View.length + 1);
                size+= Integer.BYTES + PeerData.View[j].length();
                Replication_Nodes[j] = PeerData.View[j];
            }
        }
        String Hash = null;
        if (mod == 1) {
            ipfsClass.Update_file("IPLS_directory_" + PeerData._ID + "/" +  partition + "_partial_update", new Pair<>(workers, data));
            Hash = ipfsClass.add_file("IPLS_directory_" + PeerData._ID + "/" +  partition + "_partial_update").toString();
        } else {
            if(mod == 2){
                //ipfsClass.update_file(filename, Data);
                ipfsClass.update_file("IPLS_directory_" + PeerData._ID + "/" +  partition + "_Updates",data);
                Hash = ipfsClass.add_file("IPLS_directory_" + PeerData._ID + "/" +  partition + "_Updates").toString();
            }
            else{
                ipfsClass.update_file("IPLS_directory_" + PeerData._ID + "/" +  partition + "_Gradients",data);
                Hash = ipfsClass.add_file("IPLS_directory_" + PeerData._ID + "/" +  partition + "_Gradients").toString();
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(
                + 6 * Integer.BYTES
                + id.length()
                + aggregator.length()
                + 2 + size + Hash.length());
        buf.put(Constants.MessageTags.SendPartition);
        buf.put(mod);
        buf.putInt(id.length());
        buf.put(id.getBytes());
        buf.putInt(aggregator.length());
        buf.put(aggregator.getBytes());
        buf.putInt(partition);
        buf.putInt(iteration);
        buf.putInt(workers);
        buf.putInt(Hash.length());
        if(PeerData.Gradients_Replication){
            buf.put((byte) 1);
            for(int i = 0; i < 3; i++) {
                buf.putInt(Replication_Nodes[i].length());
                buf.put(Replication_Nodes[i].getBytes());
            }
        }
        else{
            buf.put((byte) 0);
        }
        buf.put(Hash.getBytes());

        ipfs.pubsub.pub(ipfsNodeID + "Storage", Base64.getUrlEncoder().encodeToString(buf.array()));
        Thread.sleep(300);
        buf = null;

    }

    public void merge(List<String> Hashes, int iter ,int partition , String ipfsNodeID,int status) throws Exception{
        int len =1 + 3 * Integer.BYTES + Short.BYTES*(Hashes.size()+1) + PeerData._ID.length() ;
        for(int i =0 ; i < Hashes.size(); i++){
            len += Hashes.get(i).length();
        }
        ByteBuffer buf = ByteBuffer.allocate(len + 1);
        buf.put(Constants.MessageTags.Merge_Request);
        if(status == 0){
            buf.put((byte) 0);
        }
        else{
            buf.put((byte) 1);
        }
        buf.putInt(iter);
        buf.putInt(partition);
        buf.putShort((short) PeerData._ID.length());
        buf.put(PeerData._ID.getBytes());
        buf.putInt(Hashes.size());

        for(int i = 0; i < Hashes.size(); i++){
            buf.putShort((short) Hashes.get(i).length());
            buf.put(Hashes.get(i).getBytes());
        }
        ipfs.pubsub.pub(ipfsNodeID + "Storage", Base64.getUrlEncoder().encodeToString(buf.array()));
    }

    public List<Double> getPartition(String ipfsNodeID, String hash) throws Exception {
        /* Create the WantPartition message */
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 * Integer.BYTES + id.length() + hash.length());
        buf.put(Constants.MessageTags.WantPartition);
        buf.putInt(id.length());
        buf.put(id.getBytes());
        buf.putInt(hash.length());
        buf.put(hash.getBytes());

        ipfs.pubsub.pub(ipfsNodeID + "Storage", Base64.getUrlEncoder().encodeToString(buf.array()));

        return partitionsQueue.take();
    }

    public void run(){
        ByteBuffer buf;
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        byte []rawMessage;
        Sub sub = new Sub(id + "Storage", PeerData.Path, queue, true);
        sub.start();

        while (true){
            try {
                rawMessage = Utils.getRawMessage(queue);

                switch (Utils.getTag(rawMessage)) {
                    case Constants.MessageTags.DiscoverReply :
                        /* separate message from tag */
                        byte[] msg = Arrays.copyOfRange(rawMessage, 1, rawMessage.length);
                        String id = new String(msg);
                        /* add to dedicated queue */
                        ipfsNodesQueue.add(id);
                        break;
                    case Constants.MessageTags.SendPartitionReply :
                        buf = ByteBuffer.wrap(Arrays.copyOfRange(rawMessage, 1, rawMessage.length));
                        int mod = buf.get();
                        /* Read the gradients from the partition into a list */
                        int partition = buf.getInt();
                        int iter = buf.getInt();
                        int hash_size = buf.getInt();
                        byte [] hash_bytes = new byte[hash_size];
                        buf.get(hash_bytes);
                        String hash = new String(hash_bytes);
                        PeerData.DS_mtx.acquire();

                        System.out.println("Store reply " + partition);
                        PeerData.Partitions_committed.get(mod).add(new Triplet<>(new Integer(partition),iter,hash));
                        PeerData.DS_mtx.release();
                        // Do something.
                        break;
                    case Constants.MessageTags.PartitionReply :
                        /* separate message from tag and convert to Bytebuffer*/
                        buf = ByteBuffer.wrap(Arrays.copyOfRange(rawMessage, 1, rawMessage.length));

                        /* Read the gradients from the partition into a list */
                        int size = buf.getInt();

                        List<Double> l = new ArrayList<>();
                        for (int i = 0; i < size; i++)
                            l.add(buf.getDouble());

                        partitionsQueue.add(l);

                    default : throw new IllegalStateException("Unexpected value: " + Utils.getTag(rawMessage));
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
