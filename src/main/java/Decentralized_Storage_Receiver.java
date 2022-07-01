import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.api.NamedStreamable;
import io.ipfs.api.Sub;
import io.ipfs.multihash.Multihash;
import org.javatuples.*;
import org.web3j.abi.datatypes.Int;

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
    Map<String,List<String>> Replicas_Ack_List = new HashMap<>();
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
        PeerData._ID = id;
        PeerData.Path = multiaddr;
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        BlockingQueue<Septet<Byte,Byte, Integer,Integer,String,String,String>> task_queue = new LinkedBlockingQueue<>();
        for(int i = 0; i < 5; i++){
            Downloader downloader_thread = new Downloader(task_queue);
            downloader_thread.start();
        }
        byte[] rawMessage;
        Sub sub = new Sub(id + "Storage", multiaddr, queue, true);
        sub.start();
        System.out.println("Receiver Initialized");
        InitSem.release();
        int fileNo = 1;
        while (true){
            try{
                System.out.println("Waiting");
                rawMessage = Utils.getRawMessage(queue);
                System.out.println("MSG RECEIVED");
                //switch (Utils.getTag(rawMessage)) {
                byte tag = Utils.getTag(rawMessage);
                if( tag == Constants.MessageTags.SendPartition || tag==Constants.MessageTags.Replication_Request) {
                    /* separate message from tag and convert to ByteBuffer*/
                    String[] Replicas = new String[3];
                    buf = ByteBuffer.wrap(Arrays.copyOfRange(rawMessage, 1, rawMessage.length));
                    byte mod = buf.get();
                    /* get the IDs of the node that calculated the partition and of the responsible aggregator */
                    len = buf.getInt();
                    byte[] trainerBytes = new byte[len];
                    buf.get(trainerBytes);
                    String trainer = new String(trainerBytes);

                    len = buf.getInt();
                    byte[] aggregatorBytes = new byte[len];
                    buf.get(aggregatorBytes);
                    String aggregator = new String(aggregatorBytes);

                    /* get partition and iteration numbers */
                    int partition = buf.getInt();
                    int iteration = buf.getInt();

                    /* create a file with the gradients and add it to ipfs */
                    String filename = Constants.Directory + mod + "-" + trainer;
                    fileNo++;
                    int workers = buf.getInt();
                    len = buf.getInt();
                    System.out.println(len);
                    byte replication = buf.get();
                    if(replication == 1){
                        int str_size;
                        for(int i = 0; i < 3; i++){
                            str_size = buf.getInt();
                            byte[] Replica_Node = new byte[str_size];
                            buf.get(Replica_Node);
                            Replicas[i] = new String(Replica_Node);
                        }
                    }

                    byte[] HashBytes = new byte[len];
                    buf.get(HashBytes);
                    String hash = new String(HashBytes);
                    /*
                    Data = new double[len];
                    for (int i = 0; i < len; i++) {
                        Data[i] = buf.getDouble();
                    }
                    File file = new File(filename);
                    if (!file.exists()) {
                        file.createNewFile();
                    }

                    //MerkleNode addResult = ipfs.add(new NamedStreamable.FileWrapper(new File(filename))).get(0);
                    int b = (int) Instant.now().getEpochSecond();
                    if (mod == 1) {
                        ipfsClass.Update_file(filename, new Pair<>(workers, Data));
                    } else {
                        ipfsClass.update_file(filename, Data);
                    }

                    hash = ipfsClass.add_file(filename).toString();
                    int e = (int) Instant.now().getEpochSecond();

                    */

                    partitionHashMap.put(hash, partition);
                    iterationHashMap.put(hash, iteration);
                    trainerHashMap.put(hash, trainer);
                    aggregatorHashMap.put(hash, aggregator);
                    //List<String> arr = new ArrayList<>();
                    //arr.add(hash);
                    //arr.add(id);
                    //System.out.println("Added " + partition_type.get(mod) + ", " + partition + " in iteration " + iteration + " with hash: " + hash + " time : " + (e - b));
                    //System.out.println(arr);
                    //ipfs.pubsub.pub(aggregator,ipfsClass.Marshall_Packet(arr,true,(short) 56));

                    if(tag == Constants.MessageTags.SendPartition) {
                        if(replication == 0){
                            task_queue.add(new Septet<>(Constants.MessageTags.SendPartitionReply,mod,partition,iteration,hash,trainer,aggregator));

                        }
                        else{
                            rawMessage[0] = Constants.MessageTags.ReplicationReply;
                            Replicas_Ack_List.put(hash,new ArrayList<>());
                            for(int i = 1; i < 3; i++){
                                ipfs.pubsub.pub(Replicas[i]+"Storage",Base64.getUrlEncoder().encodeToString(rawMessage));
                                Replicas_Ack_List.get(hash).add(Replicas[i]);
                            }
                        }
                    }
                    else{
                        // do some checks
                        replyMsgBuffer = ByteBuffer.allocate(2 + 4 * Integer.BYTES + hash.length() + Replicas[0].length());
                        replyMsgBuffer.put(Constants.MessageTags.ReplicationReply);
                        replyMsgBuffer.putInt(hash.length());
                        replyMsgBuffer.put(hash.getBytes());
                        replyMsgBuffer.putInt(Replicas[0].length());
                        replyMsgBuffer.put(Replicas[0].getBytes());

                        ipfs.pubsub.pub(Replicas[0]+"Storage",Base64.getUrlEncoder().encodeToString(replyMsgBuffer.array()));
                    }
                    //System.gc();
                }
                else if(tag == Constants.MessageTags.WantPartition) {
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

                    for (String d : lines) {
                        replyMsgBuffer.putDouble(Double.parseDouble(d));
                    }

                    /* reply */
                    ipfs.pubsub.pub(iplsNodeID + "Storage", Base64.getUrlEncoder().encodeToString(replyMsgBuffer.array()));
                }
                else if(tag == Constants.MessageTags.Merge_Request) {
                    System.out.println("Got merge request");
                    buf = ByteBuffer.wrap(Arrays.copyOfRange(rawMessage, 1, rawMessage.length));
                    byte status = buf.get();
                    int iter = buf.getInt();
                    int partition = buf.getInt();
                    len = buf.getShort();
                    byte[] peerID = new byte[len];
                    buf.get(peerID);
                    String peer = new String(peerID);
                    int arr_size = buf.getInt();
                    List<String> Hashes = new ArrayList<>();
                    for (int i = 0; i < arr_size; i++) {
                        len = buf.getShort();
                        byte[] hash_bytes = new byte[len];
                        buf.get(hash_bytes);
                        Hashes.add(new String(hash_bytes));
                    }
                    double[] Aggregation,Gradient;
                    if(status == 0){
                        Aggregation = AuxiliaryIpfs.GetParameters(Hashes.get(0));

                        for (int i = 1; i < Hashes.size(); i++) {
                            Gradient = AuxiliaryIpfs.GetParameters(Hashes.get(i));
                            for (int j = 0; j < Gradient.length; j++) {
                                Aggregation[j] += Gradient[j];
                            }
                        }
                    }
                    else{
                        Aggregation = AuxiliaryIpfs.Download_Partial_Updates(Hashes.get(0)).getValue1();
                        for (int i = 1; i < Hashes.size(); i++) {
                            Gradient = AuxiliaryIpfs.Download_Partial_Updates(Hashes.get(i)).getValue1();
                            for (int j = 0; j < Gradient.length; j++) {
                                Aggregation[j] += Gradient[j];
                            }
                        }
                    }
                    ipfsClass.update_file("IPLS_directory_" + peer + "/" + partition + "_partial_aggregation", Aggregation);
                    String Hash = ipfsClass.add_file("IPLS_directory_" + peer + "/" + partition + "_partial_aggregation").toString();
                    Hashes.add(0, Hash);
                    if(status == 0){
                        AuxiliaryIpfs.send(peer, AuxiliaryIpfs.Marshall_Packet((short) 0, partition, iter, peer, Hashes));
                    }
                    else{
                        AuxiliaryIpfs.send(peer, AuxiliaryIpfs.Marshall_Packet((short) 2, partition, iter, peer, Hashes));
                    }
                    System.out.println("Got merge request from " + peer + " for (" + iter + " ," + partition + " )" + " , " + Hashes);
                    Aggregation = null;
                    Gradient = null;
                    //System.gc();
                }
                else if(tag == Constants.MessageTags.ReplicationReply){
                    buf = ByteBuffer.wrap(Arrays.copyOfRange(rawMessage, 1, rawMessage.length));
                    len = buf.getInt();
                    byte[] hash = new byte[len];
                    buf.get(hash);
                    len = buf.getInt();
                    byte[] node = new byte[len];
                    buf.get(node);
                    Replicas_Ack_List.get(new String(hash)).remove(new String(node));
                    if(Replicas_Ack_List.get(new String(hash)).size() == 0){
                        replyMsgBuffer = ByteBuffer.allocate(2 + 3 * Integer.BYTES + new String(hash).length());
                        replyMsgBuffer.put(Constants.MessageTags.SendPartitionReply);
                        replyMsgBuffer.put((byte) 0);
                        replyMsgBuffer.putInt(partitionHashMap.get(new String(hash)));
                        replyMsgBuffer.putInt(iterationHashMap.get(new String(hash)));
                        replyMsgBuffer.putInt(new String(hash).length());
                        replyMsgBuffer.put(new String(hash).getBytes());
                        ipfs.pubsub.pub(trainerHashMap.get(new String(hash)) + "Storage", Base64.getUrlEncoder().encodeToString(replyMsgBuffer.array()));
                        //send ack to the node
                        partitionHashMap.remove(new String(hash));
                        iterationHashMap.remove(new String(hash));
                        trainerHashMap.remove(new String(hash));
                        Replicas_Ack_List.remove(new String(hash));
                    }

                }
                else{
                    throw new IllegalStateException("Unexpected value: " + Utils.getTag(rawMessage));
                }
                System.gc();
            } catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
