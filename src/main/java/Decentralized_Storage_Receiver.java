import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.api.NamedStreamable;
import io.ipfs.api.Sub;
import io.ipfs.multihash.Multihash;
import org.javatuples.Pair;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;


public class Decentralized_Storage_Receiver extends Thread{
    IPFS ipfs;
    MyIPFSClass ipfsClass;
    String multiaddr;
    Semaphore InitSem;
    MyIPFSClass AuxiliaryIpfs;
    private final String id; // for convenience

    public Decentralized_Storage_Receiver(String multiaddr,Semaphore InitSem) throws IOException {
        ipfs = new IPFS(multiaddr);
        ipfsClass = new MyIPFSClass(multiaddr);
        id = (String) ipfs.id().get("ID");

        this.multiaddr = multiaddr;
        this.InitSem = InitSem;
        AuxiliaryIpfs = new MyIPFSClass(multiaddr);
        /* create ipls directory if it doesn't exist */
        Files.createDirectories(Paths.get(Constants.Directory));

    }

    public void run(){
        Map<Byte,String> partition_type = new HashMap<>();
        partition_type.put((byte)0,"Gradient");
        partition_type.put((byte)1,"Partial Update");
        partition_type.put((byte)2,"Update");

        /*
        Monitor a dedicated discovery topic.
        IPLS nodes send their id in the discovery topic,
        and every IPFS node replies with his ID.
        */

        /* Both ipls and ipfs nodes receive messages in a topic described by their id */

        /* hashmaps storing information about a partition, referenced by its hash in ipfs*/
        ByteBuffer replyMsgBuffer;
        HashMap<String, Integer> partitionHashMap = new HashMap<>();
        HashMap<String, Integer> iterationHashMap = new HashMap<>();
        HashMap<String, String> trainerHashMap = new HashMap<>();
        HashMap<String, String> aggregatorHashMap = new HashMap<>();
        ByteBuffer buf;
        double[] Data;

        int len;
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        byte[] rawMessage;
        Sub sub = new Sub(id + "Storage", multiaddr, queue, true);
        sub.start();
        System.out.println("Receiver Initialized");
        InitSem.release();
        int fileNo = 1;
        while (true){
            try{
                rawMessage = Utils.getRawMessage(queue);
                switch (Utils.getTag(rawMessage)) {
                    case Constants.MessageTags.SendPartition :
                        /* separate message from tag and convert to ByteBuffer*/
                        buf = ByteBuffer.wrap(Arrays.copyOfRange(rawMessage, 1, rawMessage.length));
                        byte mod = buf.get();
                        /* get the IDs of the node that calculated the partition and of the responsible aggregator */
                        len = buf.getInt();
                        byte[] trainerBytes = new byte[len];
                        buf.get(trainerBytes);
                        String trainer = new String(trainerBytes);

                        len = buf.getInt();
                        byte [] aggregatorBytes = new byte[len];
                        buf.get(aggregatorBytes);
                        String aggregator = new String(aggregatorBytes);

                        /* get partition and iteration numbers */
                        int partition = buf.getInt();
                        int iteration = buf.getInt();

                        /* create a file with the gradients and add it to ipfs */
                        String filename = Constants.Directory + mod  + "-" + trainer;
                        fileNo++;
                        int workers = buf.getInt();
                        len = buf.getInt();
                        System.out.println(len);

                        Data = new double[len];
                        for (int i = 0; i < len; i++) {
                            Data[i] = buf.getDouble();
                        }
                        File file = new File(filename);
                        if(!file.exists()){file.createNewFile();}
                        //MerkleNode addResult = ipfs.add(new NamedStreamable.FileWrapper(new File(filename))).get(0);
                        int b = (int) Instant.now().getEpochSecond();
                        if(mod == 1){
                            ipfsClass.Update_file(filename,new Pair<>(workers,Data));
                        }
                        else{
                            ipfsClass.update_file(filename,Data);
                        }
                        /* fill the hashmaps */
                        String hash = ipfsClass.add_file(filename).toString();
                        int e = (int) Instant.now().getEpochSecond();

                        //partitionHashMap.put(hash, partition);
                        //iterationHashMap.put(hash, iteration);
                        //trainerHashMap.put(hash, trainer);
                        //aggregatorHashMap.put(hash, aggregator);

                        System.out.println("Added "+partition_type.get(mod)+ ", " + partition + " in iteration " + iteration + " with hash: " + hash + " time : " + (e-b));

                        replyMsgBuffer = ByteBuffer.allocate(2 + 3*Integer.BYTES + hash.length());
                        replyMsgBuffer.put(Constants.MessageTags.SendPartitionReply);
                        replyMsgBuffer.put(mod);
                        replyMsgBuffer.putInt(partition);
                        replyMsgBuffer.putInt(iteration);
                        replyMsgBuffer.putInt(hash.length());
                        replyMsgBuffer.put(hash.getBytes());
                        ipfs.pubsub.pub(trainer + "Storage",Base64.getUrlEncoder().encodeToString(replyMsgBuffer.array()));
                        Data = null;
                        buf = null;
                        System.gc();
                        break;

                    case Constants.MessageTags.WantPartition:
                        /* separate message from tag and convert to ByteBuffer*/
                        buf = ByteBuffer.wrap(Arrays.copyOfRange(rawMessage, 1, rawMessage.length));

                        /* read ipls node id and requested hash */
                        len = buf.getInt();
                        byte[] iplsNodeIDBytes = new byte[len];
                        buf.get(iplsNodeIDBytes);
                        len = buf.getInt();
                        byte[] requestedHashBytes = new byte[len];
                        buf.get(requestedHashBytes);
                        /* Convert to strings */
                        String iplsNodeID = new String(iplsNodeIDBytes);
                        String requestedHash = new String(requestedHashBytes);

                        /* read partition contents */
                        byte[] fileContents = ipfs.cat(String.valueOf(Multihash.fromBase58(requestedHash)));
                        String f = new String(fileContents);
                        String[] lines = f.split("\\s");

                        /* create reply message buffer */
                        replyMsgBuffer = ByteBuffer.allocate(1 + Integer.BYTES + Double.BYTES * lines.length);
                        replyMsgBuffer.put(Constants.MessageTags.PartitionReply);
                        replyMsgBuffer.putInt(lines.length);

                        for (String d : lines){
                            replyMsgBuffer.putDouble(Double.parseDouble(d));
                        }

                        /* reply */
                        ipfs.pubsub.pub(iplsNodeID + "Storage", Base64.getUrlEncoder().encodeToString(replyMsgBuffer.array()));
                        break;
                    case Constants.MessageTags.Merge_Request:
                        buf = ByteBuffer.wrap(Arrays.copyOfRange(rawMessage,1,rawMessage.length));
                        int iter = buf.getInt();
                        partition = buf.getInt();
                        len = buf.getShort();
                        byte[] peerID = new byte[len];
                        buf.get(peerID);
                        String peer = new String(peerID);
                        int arr_size = buf.getInt();
                        List<String> Hashes = new ArrayList<>();
                        for(int i = 0; i < arr_size; i++){
                            len = buf.getShort();
                            byte[] hash_bytes = new byte[len];
                            buf.get(hash_bytes);
                            Hashes.add(new String(hash_bytes));
                        }
                        System.out.println("Got merge request from " + peer + " for (" + iter+ " ,"+partition + " )" + " , " + Hashes );
                        List<Double> Aggregation = (List<Double>)AuxiliaryIpfs.DownloadParameters(Hashes.get(0));
                        List<Double> Gradient = new ArrayList<>();
                        for(int i  =1; i < Hashes.size(); i++){
                            Gradient = (List<Double>) AuxiliaryIpfs.DownloadParameters(Hashes.get(i));
                            for(int j = 0; j < Gradient.size(); j++){
                                Aggregation.set(j,Aggregation.get(j) + Gradient.get(j));
                            }
                        }
                        ipfsClass.Update_file("IPLS_directory_" + peer + "/" +  partition + "_partial_aggregation",Aggregation);
                        String Hash = ipfsClass.add_file("IPLS_directory_" + peer + "/" +  partition + "_partial_aggregation").toString();
                        Hashes.add(0,Hash);
                        AuxiliaryIpfs.send(peer,AuxiliaryIpfs.Marshall_Packet((short)0,partition,iter,peer,Hashes));
                        Aggregation = new ArrayList<>();
                        Gradient = new ArrayList<>();
                        System.gc();
                        break;

                    default : throw new IllegalStateException("Unexpected value: " + Utils.getTag(rawMessage));
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
