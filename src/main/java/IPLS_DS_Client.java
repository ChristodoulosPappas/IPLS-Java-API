import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;

import io.ipfs.api.*;
import io.ipfs.multibase.Base58;
import org.javatuples.Triplet;
import org.json.JSONObject;

public class IPLS_DS_Client extends Thread{
    IPFS ipfs;
    int curRound;
    Sub replySub;
    String myId, dsId;
    BlockingQueue<String> msgQueue;
    List<BlockingQueue<String>> Queues;


    public IPLS_DS_Client(IPFS ipfs) throws IOException {
        this.ipfs = ipfs;
        myId = (String)ipfs.id().get("ID");
        this.dsId = dsId;
        this.curRound = curRound;
        Queues = new ArrayList<>();
        for(int i = 0; i < 6; i++){
            Queues.add(new LinkedBlockingDeque<>());
        }

        msgQueue = new LinkedBlockingQueue<>();
        replySub = new Sub(myId + "_DS", PeerData.Path,
                           msgQueue, true);
        replySub.start();
    }

    public void storeGradients(List<byte[]> aggregators,
                               List<Integer> partitions,
                               List<byte[]> hashes,
                               int iter) throws Exception {
        ByteBuffer storeRequest;
        int reqSize;

        reqSize = 1 + Integer.BYTES + Integer.BYTES + myId.getBytes().length + Integer.BYTES;

        // Upload the gradients into IPFS and get the hashes
        for (int i = 0 ; i < partitions.size() ; i++) {
            // Update the request size
            reqSize += Integer.BYTES;
            reqSize += aggregators.get(i).length;
            reqSize += Integer.BYTES;
            reqSize += Integer.BYTES;
            reqSize += hashes.get(i).length;
        }

        // Build the message
        storeRequest = ByteBuffer.allocate(reqSize);

        storeRequest.put(MessageCodes.STORE_GRADIENT);

        storeRequest.putInt(iter);

        storeRequest.putInt(myId.getBytes().length);
        storeRequest.put(myId.getBytes());

        storeRequest.putInt(partitions.size());
        for (int i = 0 ; i < partitions.size() ; i++) {
            storeRequest.putInt(aggregators.get(i).length);
            storeRequest.put(aggregators.get(i));
            storeRequest.putInt(partitions.get(i));
            storeRequest.putInt(hashes.get(i).length);
            storeRequest.put(hashes.get(i));
        }

        // Send the hash and wait for the reply
        ipfs.pubsub.pub("IPLSDS", Base64.getUrlEncoder().encodeToString(storeRequest.array()));

        //System.out.println("GRADIENTS SENT");

        // Wait until you get the reply
        do {
            JSONObject jobj = new JSONObject(Queues.get(new Integer(MessageCodes.STORE_GRADIENT/2)).take());
            String encoded = (String) jobj.get("data");
            Base64.Decoder dec = Base64.getUrlDecoder();
            ByteBuffer reply = ByteBuffer.wrap(dec.decode(dec.decode(encoded)));
            //System.out.println("ACK");
            reply.get();
            byte code = reply.get();
            if (code == MessageCodes.ROUND_MISMATCH) {
                int correctRound = reply.getInt();
                if (correctRound > iter)
                    throw new RoundMismatchException("round mismatch", correctRound);
                else {
                    System.out.println("Lower DS roundnum");
                    continue;
                }
            }
            else if (code != MessageCodes.STORE_GRADIENT_REPLY)
                continue;

            int roundNum = reply.getInt();
            if (roundNum == iter)
                break;

            //System.out.println(roundNum);

            System.out.println("WRONG ROUND");
        } while (true);
    }
    /*
    public void storeGradients(List<byte[]> aggregators,
                               List<Integer> partitions,
                               List<byte[]> hashes) throws Exception {
        ByteBuffer storeRequest;
        int reqSize;

        reqSize = 1 + Integer.BYTES + Integer.BYTES + myId.getBytes().length + Integer.BYTES;

        // Upload the gradients into IPFS and get the hashes
        for (int i = 0 ; i < partitions.size() ; i++) {
            // Update the request size
            reqSize += Integer.BYTES;
            reqSize += aggregators.get(i).length;
            reqSize += Integer.BYTES;
            reqSize += Integer.BYTES;
            reqSize += hashes.get(i).length;
        }

        // Build the message
        storeRequest = ByteBuffer.allocate(reqSize);

        storeRequest.put(MessageCodes.STORE_GRADIENT);

        storeRequest.putInt(curRound);

        storeRequest.putInt(myId.getBytes().length);
        storeRequest.put(myId.getBytes());

        storeRequest.putInt(partitions.size());
        for (int i = 0 ; i < partitions.size() ; i++) {
            storeRequest.putInt(aggregators.get(i).length);
            storeRequest.put(aggregators.get(i));
            storeRequest.putInt(partitions.get(i));
            storeRequest.putInt(hashes.get(i).length);
            storeRequest.put(hashes.get(i));
        }

        // Send the hash and wait for the reply
        ipfs.pubsub.pub("IPLSDS", Base64.getUrlEncoder().encodeToString(storeRequest.array()));

        System.out.println("GRADIENTS SENT");

        // Wait until you get the reply
        do {
            JSONObject jobj = new JSONObject(msgQueue.take());
            String encoded = (String) jobj.get("data");
            Base64.Decoder dec = Base64.getUrlDecoder();
            ByteBuffer reply = ByteBuffer.wrap(dec.decode(dec.decode(encoded)));

            byte code = reply.get();
            if (code == MessageCodes.ROUND_MISMATCH) {
                int correctRound = reply.getInt();
                if (correctRound > curRound)
                    throw new RoundMismatchException("round mismatch", correctRound);
                else {
                    System.out.println("Lower DS roundnum");
                    continue;
                }
            }
            else if (code != MessageCodes.STORE_GRADIENT_REPLY)
                continue;

            int roundNum = reply.getInt();
            if (roundNum == curRound)
                break;

            System.out.println(roundNum);

            System.out.println("WRONG ROUND");
        } while (true);
    }


    */
    public void storePartials(int partition, byte[] hash, List<byte[]> replicas,int iter) throws Exception {
        multicastHash(partition, hash, replicas, MessageCodes.STORE_PARTIAL,
                      MessageCodes.STORE_PARTIAL_REPLY,iter);
    }

    private  Map<Integer, List<Pair<byte[], byte[]>>> getPartitionHashes(IPFS ipfs,
                                                                         Set<Integer> partitions,
                                                                         byte reqCode,
                                                                         byte replyCode,
                                                                         int iter)
            throws Exception {
        Map<Integer, List<Pair<byte[], byte[]>>> hashes = new HashMap<>();
        ByteBuffer getRequest;
        int reqSize, len, hashNum;
        byte[] senderId, hash;
        byte code;
        int recvPartition;
        Set<Integer> recvPartitions = new HashSet<>();

        // Set a GET-GRADIENTS request for every partition
        for (int p : partitions) {
            reqSize = 1 + Integer.BYTES + Integer.BYTES + myId.getBytes().length + Integer.BYTES;

            getRequest = ByteBuffer.allocate(reqSize);

            getRequest.put(reqCode);

            getRequest.putInt(iter);

            getRequest.putInt(myId.getBytes().length);
            getRequest.put(myId.getBytes());
            getRequest.putInt(p);

            ipfs.pubsub.pub("IPLSDS", Base64.getUrlEncoder().encodeToString(getRequest.array()));
        }

        // Wait until you get a reply for every partition
        do {
            JSONObject jobj = new JSONObject(Queues.get(new Integer(reqCode/2)).take());
            String encoded = (String) jobj.get("data");
            Base64.Decoder dec = Base64.getUrlDecoder();
            ByteBuffer reply = ByteBuffer.wrap(dec.decode(dec.decode(encoded)));

            //System.out.println("GOT REPLY");

            reply.get();
            code = reply.get();
            //System.out.println(code);
            if (code == MessageCodes.ROUND_MISMATCH) {
                int correctRound = reply.getInt();
                if (correctRound > iter)
                    throw new RoundMismatchException("round mismatch", correctRound);
                else {
                    System.out.println("Lower DS roundnum");
                    continue;
                }
            }
            else if (code != replyCode)
                continue;

            int roundNum = reply.getInt();
            if (roundNum != iter) {
                System.out.println("WRONG ROUND");
                continue;
            }

            recvPartition = reply.getInt();

            hashNum = reply.getInt();

            if (hashNum == 0) {
                hashes.put(recvPartition, null);
            }
            else {
                hashes.put(recvPartition, new ArrayList<>());

                for (int i = 0 ; i < hashNum ; i++) {
                    len = reply.getInt();
                    senderId = new byte[len];
                    reply.get(senderId);

                    len = reply.getInt();
                    hash = new byte[len];
                    reply.get(hash);

                    hashes.get(recvPartition).add(new Pair<>(senderId, hash));
                }
            }
            recvPartitions.add(recvPartition);
        } while(!recvPartitions.equals(partitions));

        return hashes;
    }
    /*
    private  Map<Integer, List<Pair<byte[], byte[]>>> getPartitionHashes(IPFS ipfs,
                                                                        Set<Integer> partitions,
                                                                        byte reqCode,
                                                                        byte replyCode)
            throws Exception {
        Map<Integer, List<Pair<byte[], byte[]>>> hashes = new HashMap<>();
        ByteBuffer getRequest;
        int reqSize, len, hashNum;
        byte[] senderId, hash;
        byte code;
        int recvPartition;
        Set<Integer> recvPartitions = new HashSet<>();

        // Set a GET-GRADIENTS request for every partition
        for (int p : partitions) {
            reqSize = 1 + Integer.BYTES + Integer.BYTES + myId.getBytes().length + Integer.BYTES;

            getRequest = ByteBuffer.allocate(reqSize);

            getRequest.put(reqCode);

            getRequest.putInt(curRound);

            getRequest.putInt(myId.getBytes().length);
            getRequest.put(myId.getBytes());
            getRequest.putInt(p);

            ipfs.pubsub.pub("IPLSDS", Base64.getUrlEncoder().encodeToString(getRequest.array()));
        }

        // Wait until you get a reply for every partition
        do {
            JSONObject jobj = new JSONObject(msgQueue.take());
            String encoded = (String) jobj.get("data");
            Base64.Decoder dec = Base64.getUrlDecoder();
            ByteBuffer reply = ByteBuffer.wrap(dec.decode(dec.decode(encoded)));

            System.out.println("GOT REPLY");

            code = reply.get();
            if (code == MessageCodes.ROUND_MISMATCH) {
                int correctRound = reply.getInt();
                if (correctRound > curRound)
                    throw new RoundMismatchException("round mismatch", correctRound);
                else {
                    System.out.println("Lower DS roundnum");
                    continue;
                }
            }
            else if (code != replyCode)
                continue;

            int roundNum = reply.getInt();
            if (roundNum != curRound) {
                System.out.println("WRONG ROUND");
                continue;
            }

            recvPartition = reply.getInt();

            hashNum = reply.getInt();

            if (hashNum == 0) {
                hashes.put(recvPartition, null);
            }
            else {
                hashes.put(recvPartition, new ArrayList<>());

                for (int i = 0 ; i < hashNum ; i++) {
                    len = reply.getInt();
                    senderId = new byte[len];
                    reply.get(senderId);

                    len = reply.getInt();
                    hash = new byte[len];
                    reply.get(hash);

                    hashes.get(recvPartition).add(new Pair<>(senderId, hash));
                }
            }
            recvPartitions.add(recvPartition);
        } while(!recvPartitions.equals(partitions));

        return hashes;
    }


     */
    public Map<Integer, List<Pair<byte[], byte[]>>> getGradients(IPFS ipfs, Set<Integer> partitions, int iter)
            throws Exception {
        return getPartitionHashes(ipfs, partitions, MessageCodes.GET_GRADIENT,
                                  MessageCodes.GET_GRADIENT_REPLY , iter);
    }

    public Map<Integer, List<Pair<byte[], byte[]>>> getPartials(Set<Integer> partitions, int iter)
            throws Exception {
        return getPartitionHashes(ipfs, partitions, MessageCodes.GET_PARTIAL,
                MessageCodes.GET_PARTIAL_REPLY, iter);
    }

    public void storeUpdate(int partition, byte[] hash, List<byte[]> participants,int iter)
                            throws Exception {
        multicastHash(partition, hash, participants, MessageCodes.STORE_UPDATE,
                      MessageCodes.STORE_UPDATE_REPLY, iter);
    }

    public void storePartial(int partition, byte[] hash, List<byte[]> replicas, int iter)
            throws Exception {
        multicastHash(partition, hash, replicas, MessageCodes.STORE_PARTIAL,
                      MessageCodes.STORE_PARTIAL_REPLY, iter );
    }

    private void multicastHash(int partition, byte[] hash,
                               List<byte[]> recipients, byte reqCode, byte replyCode, int iter) throws Exception {
        ByteBuffer storeRequest;
        int reqSize;

        // Upload the file into IPFS
        reqSize = 1 + Integer.BYTES + Integer.BYTES + myId.getBytes().length + Integer.BYTES +
                Integer.BYTES + hash.length + Integer.BYTES;

        // For every participant we need the hash size and the hash itself
        for (byte[] recipient : recipients) {
            reqSize += Integer.BYTES;
            reqSize += recipient.length;
        }
        //System.out.println("REQ SIZE " + reqSize);
        // Build the STORE request package
        storeRequest = ByteBuffer.allocate(reqSize);

        storeRequest.put(reqCode);

        storeRequest.putInt(iter);

        storeRequest.putInt(myId.getBytes().length);
        storeRequest.put(myId.getBytes());

        storeRequest.putInt(partition);

        storeRequest.putInt(hash.length);
        storeRequest.put(hash);

        storeRequest.putInt(recipients.size());
        for (byte[] recipient : recipients) {
            storeRequest.putInt(recipient.length);
            storeRequest.put(recipient);
        }

        // Send the hash to the directory service
        ipfs.pubsub.pub("IPLSDS", Base64.getUrlEncoder().encodeToString(storeRequest.array()));

        // Wait until you get the reply
        do {
            JSONObject jobj = new JSONObject(Queues.get(new Integer(reqCode/2)).take());
            String encoded = (String) jobj.get("data");
            Base64.Decoder dec = Base64.getUrlDecoder();
            ByteBuffer reply = ByteBuffer.wrap(dec.decode(dec.decode(encoded)));
            reply.get();
            byte code = reply.get();
            if (code == MessageCodes.ROUND_MISMATCH) {
                int correctRound = reply.getInt();
                if (correctRound > iter)
                    throw new RoundMismatchException("round mismatch", correctRound);
                else {
                    System.out.println("Lower DS roundnum");
                    continue;
                }
            }
            else if (code != replyCode)
                continue;

            int roundNum = reply.getInt();
            if (roundNum == iter)
                break;

            System.out.println("WRONG ROUND");
        } while (true);
    }
    /*
    private void multicastHash(int partition, byte[] hash,
                              List<byte[]> recipients, byte reqCode, byte replyCode) throws Exception {
        ByteBuffer storeRequest;
        int reqSize;

        // Upload the file into IPFS
        reqSize = 1 + Integer.BYTES + Integer.BYTES + myId.getBytes().length + Integer.BYTES +
                  Integer.BYTES + hash.length + Integer.BYTES;

        // For every participant we need the hash size and the hash itself
        for (byte[] recipient : recipients) {
            reqSize += Integer.BYTES;
            reqSize += recipient.length;
        }

        // Build the STORE request package
        storeRequest = ByteBuffer.allocate(reqSize);

        storeRequest.put(reqCode);

        storeRequest.putInt(curRound);

        storeRequest.putInt(myId.getBytes().length);
        storeRequest.put(myId.getBytes());

        storeRequest.putInt(partition);

        storeRequest.putInt(hash.length);
        storeRequest.put(hash);

        storeRequest.putInt(recipients.size());
        for (byte[] recipient : recipients) {
            storeRequest.putInt(recipient.length);
            storeRequest.put(recipient);
        }

        // Send the hash to the directory service
        ipfs.pubsub.pub("IPLSDS", Base64.getUrlEncoder().encodeToString(storeRequest.array()));

        // Wait until you get the reply
        do {
            JSONObject jobj = new JSONObject(msgQueue.take());
            String encoded = (String) jobj.get("data");
            Base64.Decoder dec = Base64.getUrlDecoder();
            ByteBuffer reply = ByteBuffer.wrap(dec.decode(dec.decode(encoded)));

            byte code = reply.get();
            if (code == MessageCodes.ROUND_MISMATCH) {
                int correctRound = reply.getInt();
                if (correctRound > curRound)
                    throw new RoundMismatchException("round mismatch", correctRound);
                else {
                    System.out.println("Lower DS roundnum");
                    continue;
                }
            }
            else if (code != replyCode)
                continue;

            int roundNum = reply.getInt();
            if (roundNum == curRound)
                break;

            System.out.println("WRONG ROUND");
        } while (true);
    }

     */

    public List<Triplet<String, Integer, String>> getUpdates(int iter) throws Exception {
        ByteBuffer getRequest;
        int len, updateNum, partition;
        List<Triplet<String, Integer, String>> updates = new ArrayList<>();
        byte[] hash, aggregator;
        byte replyCode;

        // Build the GET request
        getRequest = ByteBuffer.allocate(1 + Integer.BYTES + Integer.BYTES +
                                         myId.getBytes().length);

        getRequest.put(MessageCodes.GET_UPDATE);
        //System.out.println(iter);
        getRequest.putInt(iter);
        getRequest.putInt(myId.getBytes().length);
        getRequest.put(myId.getBytes());

        // Sub so you can get the reply
        ipfs.pubsub.pub("IPLSDS", Base64.getUrlEncoder().encodeToString(getRequest.array()));

        // Wait until you get the reply
        do {
            JSONObject jobj = new JSONObject(Queues.get(new Integer(MessageCodes.GET_UPDATE/2)).take());
            String encoded = (String) jobj.get("data");
            Base64.Decoder dec = Base64.getUrlDecoder();
            ByteBuffer reply = ByteBuffer.wrap(dec.decode(dec.decode(encoded)));

            //System.out.println("GOT REPLY");
            reply.get();
            replyCode = reply.get();
            if (replyCode == MessageCodes.ROUND_MISMATCH) {
                int correctRound = reply.getInt();
                if (correctRound > iter)
                    throw new RoundMismatchException("round mismatch", correctRound);
                else {
                    System.out.println("Lower DS roundnum");
                    continue;
                }
            }
            else if (replyCode != MessageCodes.GET_UPDATE_REPLY)
                continue;

            int roundNum = reply.getInt();
            if (roundNum != iter) {
                System.out.println("WRONG ROUND");
                continue;
            }

            updateNum = reply.getInt();
            if (updateNum == 0) {
                updates =  null;
            }
            else {
                for (int i = 0 ; i < updateNum ; i++) {
                    len = reply.getInt();
                    hash = new byte[len];
                    reply.get(hash);

                    partition = reply.getInt();

                    len = reply.getInt();
                    aggregator = new byte[len];
                    reply.get(aggregator);

                    updates.add(new Triplet<>(Base58.encode(hash), partition, Base58.encode(aggregator)));
                }
            }
        } while(replyCode != MessageCodes.GET_UPDATE_REPLY);

        return updates;
    }
    /*
    public List<Triplet<byte[], Integer, byte[]>> getUpdates() throws Exception {
        ByteBuffer getRequest;
        int len, updateNum, partition;
        List<Triplet<byte[], Integer, byte[]>> updates = new ArrayList<>();
        byte[] hash, aggregator;
        byte replyCode;

        // Build the GET request
        getRequest = ByteBuffer.allocate(1 + Integer.BYTES + Integer.BYTES +
                                         myId.getBytes().length);

        getRequest.put(MessageCodes.GET_UPDATE);
        System.out.println(curRound);
        getRequest.putInt(curRound);
        getRequest.putInt(myId.getBytes().length);
        getRequest.put(myId.getBytes());

        // Sub so you can get the reply
        ipfs.pubsub.pub("IPLSDS", Base64.getUrlEncoder().encodeToString(getRequest.array()));

        // Wait until you get the reply
        do {
            JSONObject jobj = new JSONObject(msgQueue.take());
            String encoded = (String) jobj.get("data");
            Base64.Decoder dec = Base64.getUrlDecoder();
            ByteBuffer reply = ByteBuffer.wrap(dec.decode(dec.decode(encoded)));

            System.out.println("GOT REPLY");

            replyCode = reply.get();
            if (replyCode == MessageCodes.ROUND_MISMATCH) {
                int correctRound = reply.getInt();
                if (correctRound > curRound)
                    throw new RoundMismatchException("round mismatch", correctRound);
                else {
                    System.out.println("Lower DS roundnum");
                    continue;
                }
            }
            else if (replyCode != MessageCodes.GET_UPDATE_REPLY)
                continue;

            int roundNum = reply.getInt();
            if (roundNum != curRound) {
                System.out.println("WRONG ROUND");
                continue;
            }

            updateNum = reply.getInt();
            if (updateNum == 0) {
                updates =  null;
            }
            else {
                for (int i = 0 ; i < updateNum ; i++) {
                    len = reply.getInt();
                    hash = new byte[len];
                    reply.get(hash);

                    partition = reply.getInt();

                    len = reply.getInt();
                    aggregator = new byte[len];
                    reply.get(aggregator);

                    updates.add(new Triplet<>(hash, partition, aggregator));
                }
            }
        } while(replyCode != MessageCodes.GET_UPDATE_REPLY);

        return updates;
    }


     */

    public void run(){
        try {
            while(true){
                String raw_reply = msgQueue.take();

                JSONObject jobj = new JSONObject(raw_reply);
                String encoded = (String) jobj.get("data");
                Base64.Decoder dec = Base64.getUrlDecoder();
                ByteBuffer reply = ByteBuffer.wrap(dec.decode(dec.decode(encoded)));
                byte Task = reply.get();

                Queues.get(new Integer(Task/2)).add(raw_reply);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    /*
    public static void main(String[] args) throws Exception {
        String line;
        String[] words;
        IPFS ipfs = new IPFS("/ip4/127.0.0.1/tcp/" + args[0]);
        IPLS_DS_Client client = new IPLS_DS_Client(ipfs, args[0], 0);
        BufferedReader reader = new BufferedReader(new FileReader("DS_test"));
        List<byte[]> aggregators, replicas;
        List<Integer> partitions;
        Set<Integer> partitionSet;
        String[] subwords;
        List<byte[]> participants;
        List<Triplet<byte[], Integer, byte[]>> updates;
        List<byte[]> hashes;
        Map<Integer, List<Pair<byte[], byte[]>>> gradients, partials;

        // port is args[1]
        System.out.println(ipfs.id().get("ID") + " " + args[0]);
        do {
            try {
                line = reader.readLine();
                if(line == null){
                    break;
                }
                words = line.split(" ");
                System.out.println(line);
                Thread.sleep(2000);
                switch (words[0]) {
                    case "storeu":
                        // Get list of multihashes
                        participants = new ArrayList<>();
                        for (int i = 3; i < words.length; i++) {
                            participants.add(Base58.decode(words[i]));
                        }

                        client.storeUpdate(Integer.parseInt(words[1]),
                                           Base58.decode(words[2]), participants, client.curRound);
                        break;
                    case "getu":
                        updates = client.getUpdates(client.curRound);
                        if (updates == null) {
                            System.out.println("No updates available!");
                        } else {
                            System.out.println("~~~~~~~~ Updates Get ~~~~~~~~");
                            for (int i = 0; i < updates.size(); i++) {
                                System.out.println(updates.get(i).getValue1() + " " +
                                        Base58.encode(updates.get(i).getValue0()) + " by " +
                                        Base58.encode(updates.get(i).getValue2()));
                            }
                            System.out.println("\n");
                        }
                        break;
                    // storeg part1@aggr1@hash1 ... partN@aggrN@hashN
                    case "storeg":
                        aggregators = new ArrayList<>();
                        partitions = new ArrayList<>();
                        hashes = new ArrayList<>();
                        for (int i = 1; i < words.length; i++) {
                            subwords = words[i].split("@");
                            partitions.add(Integer.parseInt(subwords[0]));
                            aggregators.add(Base58.decode(subwords[1]));
                            hashes.add(Base58.decode(subwords[2]));
                        }

                        client.storeGradients(aggregators, partitions, hashes, client.curRound);
                        break;
                    case "getg":
                        partitionSet = new HashSet<>();

                        for (int i = 1; i < words.length; i++) {
                            partitionSet.add(Integer.parseInt(words[i]));
                        }

                        gradients = client.getGradients(ipfs, partitionSet, client.curRound);

                        System.out.println("~~~~~~~ Gradients Get ~~~~~~~~");
                        for (int partition : partitionSet) {
                            System.out.println("> " + partition);
                            if (gradients.get(partition) != null) {
                                for (Pair<byte[], byte[]> p : gradients.get(partition)) {
                                    System.out.println(Base58.encode(p.right) + " by " +
                                            new String(p.left));
                                }
                            }
                        }
                        System.out.println("");
                        break;
                    // <filename> <hash> <replica1> ... <replicaN>
                    case "storep":
                        replicas = new ArrayList<>();
                        for (int i = 3 ; i < words.length; i++) {
                            replicas.add(Base58.decode(words[i]));
                        }

                        client.storePartial(Integer.parseInt(words[1]), Base58.decode(words[2]),
                                           replicas, client.curRound);
                        break;
                    case "getp":
                        partitionSet = new HashSet<>();

                        for (int i = 1; i < words.length; i++) {
                            partitionSet.add(Integer.parseInt(words[i]));
                        }

                        partials = client.getPartials(partitionSet,client.curRound);
                        System.out.println("PARTIALS : "  + partials);
                        break;
                    case "round":
                        //client.setCurRound(Integer.parseInt(words[1]),client.curRound);
                        break;
                    default:
                        System.out.println("WRONG COMMsAND!");
                }
            }
            catch (RoundMismatchException e) {
                System.out.println(e.getMessage());
                // adjust the round
                //client.setCurRound(e.getCurRound());
            }
        } while(true);
    }

     */
}
