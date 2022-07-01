import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.nio.ByteBuffer;

import io.ipfs.api.*;
import io.ipfs.multibase.Base58;
//import org.jetbrains.annotations.NotNull;
//import org.jetbrains.annotations.Nullable;
import org.javatuples.Triplet;
import org.json.JSONObject;
import org.web3j.abi.datatypes.Int;

public class IPLS_DS extends Thread{
    private Map<String, Map<String, Pair<Integer, String>>> updateHashes;
    private Map<Pair<String, Integer>, Map<String, String>> gradientHashes , stored_gradient_Hashes , partialHashes, stored_partial_Hashes;
    private Sub sub;
    private Map<Integer, org.javatuples.Pair<String,String>> Updates = new HashMap<>();
    private BlockingQueue<String> msgQueue;
    private BlockingQueue<Triplet<String,String,Integer>> udpQueue;
    private IPFS ipfs;
    DS_receiver rcv;
    String IP;
    int port;
    int curRound;
    boolean isDestructiveGet;
    boolean BatchReply = true;

    public IPLS_DS(String address, boolean isDestructiveGet) {
        updateHashes = new HashMap<>();
        gradientHashes = new HashMap<>();
        partialHashes = new HashMap<>();
        stored_gradient_Hashes = new HashMap<>();
        stored_partial_Hashes = new HashMap<>();

        this.curRound = curRound;
        this.isDestructiveGet = isDestructiveGet;

        msgQueue = new LinkedBlockingQueue<>();
        udpQueue = new LinkedBlockingQueue<>();
        try {
            ipfs = new IPFS(address);
            if(PeerData.DS_udp_comm){
                rcv = new DS_receiver(udpQueue);
                rcv.start();
            }
            else{
                sub = new Sub("IPLSDS", address,
                        msgQueue, true);
                sub.start();
            }
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }




    public void set_round(int round){
        curRound = round;
    }
    public void clear_updates_table(){
        Updates = new HashMap<>();
    }

    public void storePartials(byte[] clientId, List<byte[]> replicas, int partition, byte[] hash) {
        Pair<String, Integer> key;

        for (int i = 0 ; i < replicas.size() ; i++) {
            key = new Pair<>(Base58.encode(replicas.get(i)), partition);

            // check if a list exists for this key, if not, allocate it
            if (!partialHashes.containsKey(key)) {
                System.out.println(key.left + ", " + key.right + " didn't exist!");
                partialHashes.put(key, new HashMap<>());
            }
            if(!stored_partial_Hashes.containsKey(key)){
                stored_partial_Hashes.put(key,new HashMap<>());
            }

            // Check if a hash by this sender was sent before
            if (partialHashes.get(key).containsKey(Base58.encode(clientId))) {
                continue;
            }

            partialHashes.get(key).put(new String(clientId), Base58.encode(hash));
            stored_partial_Hashes.get(key).put(new String(clientId), Base58.encode(hash));
        }

        System.out.println("PARTIALS STORED!");
    }

    // aggregators[i], partitions[i] -> hashes[i]
    public void storeGradients(byte[] clientId, List<byte[]> aggregators,
                               List<Integer> partitions, List<byte[]> hashes) {
        Pair<String, Integer> key;

        for (int i = 0 ; i < aggregators.size() ; i++) {
            key = new Pair<>(Base58.encode(aggregators.get(i)), partitions.get(i));

            // check if a list exists for this key, if not, allocate it
            if (!gradientHashes.containsKey(key)) {
                System.out.println(key.left + ", " + key.right + " didn't exist!");
                gradientHashes.put(key, new HashMap<>());

            }
            if(!stored_gradient_Hashes.containsKey(key)){
                stored_gradient_Hashes.put(key,new HashMap<>());
            }
            // Check if a hash by this sender was sent before
            if (gradientHashes.get(key).containsKey(Base58.encode(clientId))) {
                continue;
            }

            gradientHashes.get(key).put(new String(clientId),
                                        Base58.encode(hashes.get(i)));
            stored_gradient_Hashes.get(key).put(new String(clientId),
                                        Base58.encode(hashes.get(i)));
        }

    }

    public void storeUpdates(byte[] clientId, List<byte[]> participants, int partition,
                             byte[] hash) {
        String id;

        for (int i = 0 ; i < participants.size() ; i++) {
            id = Base58.encode(participants.get(i));

            // If a map wasn't allocated for this participant yet, do it
            if (!updateHashes.containsKey(id)) {
                System.out.println(id + " didn't exist!");
                updateHashes.put(id, new HashMap<>());
            }

            // Add the file to this participants list (if it doesn't exist already)
            if (updateHashes.get(id).containsKey(hash))
                continue;

            updateHashes.get(id).put(Base58.encode(hash), new Pair(partition, new String(clientId)));
        }
        if(!Updates.containsKey(partition)){
            Updates.put(partition,new org.javatuples.Pair<>(Base58.encode(hash),new String(clientId)));
            System.out.println("UPDATES : " + Updates);
            if(PeerData.premature_termination){
                for(int i = 0; i < PeerData._PARTITIONS; i++){
                    if(!Updates.containsKey(i)){
                        return;
                    }
                }
                PeerData.flush = true;
            }
            //check if collected all updates and do something
        }
    }

    private <K, V> List<Pair<String, V>> getHashes(Map<K, Map<String, V>> hashStorage, K key) {
        int i = 0;
        List<Pair<String, V>> hashes = new ArrayList<>();

        // Check if any updates for this node exist
        if (!hashStorage.containsKey(key) || hashStorage.get(key).isEmpty()) {
            return null;
        }

        for (Map.Entry<String, V> e : hashStorage.get(key).entrySet()) {
            // In case there are more than 200 commitmnents then send only a batch of them
            if(BatchReply ){
                if(i == 200){
                    break;
                }
                i++;
            }
            hashes.add(new Pair<String, V>(e.getKey(), e.getValue()));
        }
        if(i < 200){
            hashStorage.get(key).clear();
        }
        else{
            for(i = 0; i < hashes.size(); i++){

                hashStorage.get(key).remove(hashes.get(i).left);
            }
            System.out.println(hashStorage.get(key).size());
        }
        //if (isDestructiveGet) {
         //   hashStorage.get(key).clear();
        //}

        return hashes;
    }

    public List<Pair<String, Pair<Integer, String>>> getUpdates(String clientId) {
        List<Pair<String,Pair<Integer,String>>> reply_list = new ArrayList<>();
        //System.out.println("Sending updates!! " + Updates);
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            if(Updates.containsKey(i)){
                reply_list.add(new Pair<>(Updates.get(i).getValue0(),new Pair<>(i,Updates.get(i).getValue1())));
            }
        }
        return reply_list;
        //return getHashes(updateHashes, clientId);
    }

    public List<Pair<String, String>> getGradients(Pair<String, Integer> key) {
        return getHashes(gradientHashes, key);
    }

    public List<Pair<String, String>> getPartials(Pair<String, Integer> key) {
        return getHashes(partialHashes, key);
    }

    public byte[] getRequest() throws InterruptedException {
        String encoded = null;
        Base64.Decoder dec = Base64.getUrlDecoder();

        if(PeerData.DS_udp_comm){
            Triplet<String,String,Integer> triplet = udpQueue.take();

            encoded = triplet.getValue0();
            IP =  triplet.getValue1();
            port =  triplet.getValue2();
            return dec.decode(encoded);
        }
        else{
            JSONObject jobj = new JSONObject(msgQueue.take());
            encoded = (String) jobj.get("data");
            return dec.decode(dec.decode(encoded));
        }
    }

    public void sendStoreReply(String clientId,byte Task, byte code, String debugMsg) throws Exception {
        ByteBuffer reply = ByteBuffer.allocate(2 + Integer.BYTES);

        reply.put(Task);
        reply.put(code);
        reply.putInt(curRound);

        if(PeerData.DS_udp_comm){
            rcv.reply(Base64.getUrlEncoder().encodeToString(reply.array()),IP,port);
        }
        else{
            ipfs.pubsub.pub(clientId+ "_DS", Base64.getUrlEncoder().encodeToString(reply.array()));
        }
        //System.out.println(debugMsg);
    }

    public void sendStoreReply(String clientId, byte code, String debugMsg) throws Exception {
        ByteBuffer reply = ByteBuffer.allocate(1 + Integer.BYTES);

        reply.put(code);
        reply.putInt(curRound);

        ipfs.pubsub.pub(clientId+ "_DS", Base64.getUrlEncoder().encodeToString(reply.array()));
        //System.out.println(debugMsg);
    }

    public void sendGetUpdatesReply(String clientId,
                                    Byte Task,
                                    List<Pair<String, Pair<Integer, String>>> updates)
            throws Exception {
        ByteBuffer reply;
        int replySize;

        if (updates != null) {
            replySize = 2 + Integer.BYTES + Integer.BYTES;

            for (int i = 0 ; i < updates.size() ; i++) {
                byte[] hash = Base58.decode(updates.get(i).left);
                int partition = updates.get(i).right.left;
                byte[] aggregator = Base58.decode(updates.get(i).right.right);

                replySize += Integer.BYTES;
                replySize += hash.length;
                replySize += Integer.BYTES;
                replySize += Integer.BYTES;
                replySize += aggregator.length;
            }

            reply = ByteBuffer.allocate(replySize);

            reply.put(Task);
            reply.put(MessageCodes.GET_UPDATE_REPLY);
            reply.putInt(curRound);
            reply.putInt(updates.size());

            for (int i = 0 ; i < updates.size() ; i++) {
                byte[] hash = Base58.decode(updates.get(i).left);
                int partition = updates.get(i).right.left;
                byte[] aggregator = Base58.decode(updates.get(i).right.right);

                reply.putInt(hash.length);
                reply.put(hash);
                reply.putInt(partition);
                reply.putInt(aggregator.length);
                reply.put(aggregator);
            }
        }
        else {
            reply = ByteBuffer.allocate(2 + + Integer.BYTES + Integer.BYTES);

            reply.put(Task);
            reply.put(MessageCodes.GET_UPDATE_REPLY);
            reply.putInt(curRound);
            reply.putInt(0);
        }
        if(PeerData.DS_udp_comm){
            rcv.reply(Base64.getUrlEncoder().encodeToString(reply.array()),IP,port);
        }
        else{
            ipfs.pubsub.pub(clientId + "_DS", Base64.getUrlEncoder().encodeToString(reply.array()));
        }
    }

    public void sendGetUpdatesReply(String clientId,
                                    List<Pair<String, Pair<Integer, String>>> updates)
                                    throws Exception {
        ByteBuffer reply;
        int replySize;

        if (updates != null) {
            replySize = 1 + Integer.BYTES + Integer.BYTES;

            for (int i = 0 ; i < updates.size() ; i++) {
                byte[] hash = Base58.decode(updates.get(i).left);
                int partition = updates.get(i).right.left;
                byte[] aggregator = Base58.decode(updates.get(i).right.right);

                replySize += Integer.BYTES;
                replySize += hash.length;
                replySize += Integer.BYTES;
                replySize += Integer.BYTES;
                replySize += aggregator.length;
            }

            reply = ByteBuffer.allocate(replySize);

            reply.put(MessageCodes.GET_UPDATE_REPLY);
            reply.putInt(curRound);
            reply.putInt(updates.size());

            for (int i = 0 ; i < updates.size() ; i++) {
                byte[] hash = Base58.decode(updates.get(i).left);
                int partition = updates.get(i).right.left;
                byte[] aggregator = Base58.decode(updates.get(i).right.right);

                reply.putInt(hash.length);
                reply.put(hash);
                reply.putInt(partition);
                reply.putInt(aggregator.length);
                reply.put(aggregator);
            }
        }
        else {
            reply = ByteBuffer.allocate(1 + + Integer.BYTES + Integer.BYTES);

            reply.put(MessageCodes.GET_UPDATE_REPLY);
            reply.putInt(curRound);
            reply.putInt(0);
        }

        if(PeerData.DS_udp_comm){
            rcv.reply(Base64.getUrlEncoder().encodeToString(reply.array()),IP,port);
        }
        else{
            ipfs.pubsub.pub(clientId + "_DS", Base64.getUrlEncoder().encodeToString(reply.array()));
        }
    }

    private void sendGetPartHashesReply(String clientId, List<Pair<String, String>> hashes,
                                        int partition, String msg ,byte Task, byte replyCode) throws Exception {
        ByteBuffer reply;
        int replySize;

        if (hashes != null) {
            replySize = 2 + Integer.BYTES + Integer.BYTES + Integer.BYTES;

            for (int i = 0 ; i < hashes.size() ; i++) {
                byte[] partId = hashes.get(i).left.getBytes();
                byte[] hash = Base58.decode(hashes.get(i).right);

                replySize += Integer.BYTES;
                replySize += partId.length;
                replySize += Integer.BYTES;
                replySize += hash.length;
            }

            reply = ByteBuffer.allocate(replySize);

            reply.put(Task);
            reply.put(replyCode);

            reply.putInt(curRound);

            reply.putInt(partition);

            reply.putInt(hashes.size());

            for (int i = 0 ; i < hashes.size() ; i++) {
                byte[] id = hashes.get(i).left.getBytes();
                byte[] hash = Base58.decode(hashes.get(i).right);

                reply.putInt(id.length);
                reply.put(id);
                reply.putInt(hash.length);
                reply.put(hash);
            }
        }
        else {
            reply = ByteBuffer.allocate(2 + Integer.BYTES + Integer.BYTES + Integer.BYTES);

            reply.put(Task);
            reply.put(replyCode);

            reply.putInt(curRound);

            reply.putInt(partition);
            reply.putInt(0);
        }
        if(PeerData.DS_udp_comm){
            System.out.println(reply.array().length);
            rcv.reply(Base64.getUrlEncoder().encodeToString(reply.array()),IP,port);
        }
        else{
            System.out.println("SENDING REPLY!!!!");
            ipfs.pubsub.pub(clientId + "_DS", Base64.getUrlEncoder().encodeToString(reply.array()));
        }

    }

    private void sendGetPartHashesReply(String clientId, List<Pair<String, String>> hashes,
                                        int partition, String msg , byte replyCode) throws Exception {
        ByteBuffer reply;
        int replySize;

        if (hashes != null) {
            replySize = 1 + Integer.BYTES + Integer.BYTES + Integer.BYTES;

            for (int i = 0 ; i < hashes.size() ; i++) {
                byte[] partId = hashes.get(i).left.getBytes();
                byte[] hash = Base58.decode(hashes.get(i).right);

                replySize += Integer.BYTES;
                replySize += partId.length;
                replySize += Integer.BYTES;
                replySize += hash.length;
            }

            reply = ByteBuffer.allocate(replySize);

            reply.put(replyCode);

            reply.putInt(curRound);

            reply.putInt(partition);

            reply.putInt(hashes.size());

            for (int i = 0 ; i < hashes.size() ; i++) {
                byte[] id = hashes.get(i).left.getBytes();
                byte[] hash = Base58.decode(hashes.get(i).right);

                reply.putInt(id.length);
                reply.put(id);
                reply.putInt(hash.length);
                reply.put(hash);
            }
        }
        else {
            reply = ByteBuffer.allocate(1 + Integer.BYTES + Integer.BYTES + Integer.BYTES);

            reply.put(replyCode);

            reply.putInt(curRound);

            reply.putInt(partition);
            reply.putInt(0);
        }
        if(PeerData.DS_udp_comm){
            rcv.reply(Base64.getUrlEncoder().encodeToString(reply.array()),IP,port);
        }
        else{
            ipfs.pubsub.pub(clientId + "_DS", Base64.getUrlEncoder().encodeToString(reply.array()));
        }
    }
    public void sendGetGradientsReply(String clientId,
                                      List<Pair<String, String>> gradients,
                                      int partition, byte Task) throws Exception {
        sendGetPartHashesReply(clientId, gradients, partition,
                "SENT GRAD GET REPLY", Task,MessageCodes.GET_GRADIENT_REPLY);
    }
    public void sendGetGradientsReply(String clientId,
                                      List<Pair<String, String>> gradients,
                                      int partition) throws Exception {
        sendGetPartHashesReply(clientId, gradients, partition,
                          "SENT GRAD GET REPLY", MessageCodes.GET_GRADIENT_REPLY);
    }

    public void sendGetPartialsReply(String clientId,
                                     List<Pair<String, String>> partials,
                                     int partition,byte Task) throws Exception {
        sendGetPartHashesReply(clientId, partials, partition,
                "SENT PARTIAL GET REPLY", Task,MessageCodes.GET_PARTIAL_REPLY);
    }

    public void sendGetPartialsReply(String clientId,
                                     List<Pair<String, String>> partials,
                                     int partition) throws Exception {
        sendGetPartHashesReply(clientId, partials, partition,
                          "SENT PARTIAL GET REPLY", MessageCodes.GET_PARTIAL_REPLY);
    }

    public void setCurRound(int curRound) {
        this.curRound = curRound;

        gradientHashes.clear();
        updateHashes.clear();
        partialHashes.clear();
    }

    public void setCurRound(int curRound, boolean clearUpdates,
                            boolean clearGradients, boolean clearPartials) {
        this.curRound = curRound;

        if (clearUpdates)
            updateHashes.clear();
        if (clearGradients)
            gradientHashes.clear();
        if (clearPartials)
            partialHashes.clear();
    }

    public void flush_ds(boolean clearGradients, boolean clearPartials){
        if(clearGradients){
            stored_gradient_Hashes.clear();
            gradientHashes.clear();
        }
        if(clearPartials){
            stored_partial_Hashes.clear();
            partialHashes.clear();
        }
    }

    public int getCurRound() {
        return curRound;
    }

    public void handleRoundMismatch(String clientId, byte Task) throws Exception {
        ByteBuffer reply;

        System.out.println("WRONG ROUND NUM!");

        reply = ByteBuffer.allocate(2 + Integer.BYTES);

        reply.put(Task);
        reply.put(MessageCodes.ROUND_MISMATCH);
        reply.putInt(curRound);
        if(PeerData.DS_udp_comm){
            rcv.reply(Base64.getUrlEncoder().encodeToString(reply.array()),IP,port);
        }
        else{
            ipfs.pubsub.pub(clientId + "_DS", Base64.getUrlEncoder().encodeToString(reply.array()));
        }
    }

    public void handleRoundMismatch(String clientId) throws Exception {
        ByteBuffer reply;

        System.out.println("WRONG ROUND NUM!");

        reply = ByteBuffer.allocate(1 + Integer.BYTES);
        reply.put(MessageCodes.ROUND_MISMATCH);
        reply.putInt(curRound);
        if(PeerData.DS_udp_comm){
            rcv.reply(Base64.getUrlEncoder().encodeToString(reply.array()),IP,port);
        }
        else{
            ipfs.pubsub.pub(clientId + "_DS", Base64.getUrlEncoder().encodeToString(reply.array()));
        }
    }

    public void run() {
        //IPLS_DS iplsds = new IPLS_DS(5002, 1, true);
        ByteBuffer request;
        int writes = 0;

        int partition;
        int len;
        byte code;
        byte[] hash;
        byte[] clientId;
        int roundNum;
        long start;
        try{
            do {
                start = System.currentTimeMillis();
                request = ByteBuffer.wrap(getRequest());


                code = request.get();

                // Check the type of the request
                switch (code) {
                    // Store Updates
                    case MessageCodes.STORE_UPDATE:

                        // Deserialize the STORE-UPDATES message
                        roundNum = request.getInt();

                        len = request.getInt();
                        clientId = new byte[len];
                        request.get(clientId);
                        System.out.println("Got update " + new Long(System.currentTimeMillis() - start) + " , " + Instant.now().getEpochSecond() + " , "  + roundNum + " , " + getCurRound());
                        if (roundNum != getCurRound()) {
                            //handleRoundMismatch(new String(clientId));
                            handleRoundMismatch(new String(clientId),MessageCodes.STORE_UPDATE);

                            continue;
                        }

                        partition = request.getInt();

                        len = request.getInt();
                        hash = new byte[len];
                        request.get(hash);

                        int partNum = request.getInt();
                        List<byte[]> participants = new ArrayList<>();
                        byte[] partId;

                        for (int i = 0; i < partNum; i++) {
                            len = request.getInt();
                            partId = new byte[len];
                            request.get(partId);
                            participants.add(partId);
                        }

                        storeUpdates(clientId, participants, partition, hash);
                        //sendStoreReply(new String(clientId), MessageCodes.STORE_UPDATE_REPLY,
                        //        "SENT STORE UPDATE REPLY");


                        sendStoreReply(new String(clientId), MessageCodes.STORE_UPDATE,MessageCodes.STORE_UPDATE_REPLY,
                                "SENT STORE UPDATE REPLY");
                        break;
                    // HASH GET
                    case MessageCodes.GET_UPDATE:
                        // Deserialize the GET-UPDATES message
                        roundNum = request.getInt();

                        len = request.getInt();
                        clientId = new byte[len];
                        request.get(clientId);

                        if (roundNum != getCurRound()) {
                            //handleRoundMismatch(new String(clientId));
                            handleRoundMismatch(new String(clientId),MessageCodes.GET_UPDATE);
                            continue;
                        }

                        List<Pair<String, Pair<Integer, String>>> updates = getUpdates(
                                new String(clientId));
                        //sendGetUpdatesReply(new String(clientId), updates);
                        sendGetUpdatesReply(new String(clientId), MessageCodes.GET_UPDATE,updates);

                        break;
                    case MessageCodes.STORE_GRADIENT:
                        writes++;
                        //System.out.println(writes);
                        //System.out.println("Got gradient " + new Long(System.currentTimeMillis() - start));
                        // Deserialize the STORE-GRADIENTS message
                        roundNum = request.getInt();
                        //System.out.println(roundNum);
                        len = request.getInt();
                        clientId = new byte[len];
                        request.get(clientId);

                        if (roundNum != getCurRound()) {
                            //handleRoundMismatch(new String(clientId));
                            System.out.println("ROUND MISMATCH");
                            handleRoundMismatch(new String(clientId),MessageCodes.STORE_GRADIENT);
                            continue;
                        }

                        int gradNum = request.getInt();
                        List<byte[]> aggregators = new ArrayList<>();
                        List<Integer> partitions = new ArrayList<>();
                        List<byte[]> hashes = new ArrayList<>();
                        byte[] aggregator;
                        //System.out.println("Gradnum : " + gradNum);
                        for (int i = 0; i < gradNum; i++) {
                            len = request.getInt();
                            aggregator = new byte[len];
                            request.get(aggregator);
                            aggregators.add(aggregator);

                            partition = request.getInt();
                            partitions.add(partition);
                            //System.out.println("Aggregator : " + new String(aggregator) + " , " + partition);
                            len = request.getInt();
                            hash = new byte[len];
                            request.get(hash);
                            hashes.add(hash);
                        }
                        long stime = System.currentTimeMillis();

                        storeGradients(clientId, aggregators, partitions, hashes);
                        long end = System.currentTimeMillis();
                        System.out.println("Insert time : " + new Long(end-stime)  + " , " + writes);
                        //sendStoreReply(new String(clientId), MessageCodes.STORE_GRADIENT_REPLY,
                         //       "SENT STORE GRADIENTS REPLY");
                        sendStoreReply(new String(clientId), MessageCodes.STORE_GRADIENT,  MessageCodes.STORE_GRADIENT_REPLY,
                                "SENT STORE GRADIENTS REPLY");
                        break;
                    case MessageCodes.GET_GRADIENT:
                        // Deserialize the GET-GRADIENTS message
                        roundNum = request.getInt();

                        len = request.getInt();
                        clientId = new byte[len];
                        request.get(clientId);

                        if (roundNum != getCurRound()) {
                            //handleRoundMismatch(new String(clientId));
                            handleRoundMismatch(new String(clientId),MessageCodes.GET_GRADIENT);
                            continue;
                        }

                        partition = request.getInt();

                        List<Pair<String, String>> gradients = getGradients
                                (new Pair<>(new String(clientId), partition));
                        //sendGetGradientsReply(new String(clientId), gradients, partition);
                        sendGetGradientsReply(new String(clientId), gradients, partition,MessageCodes.GET_GRADIENT );
                        break;
                    case MessageCodes.STORE_PARTIAL:
                        System.out.println("Got partial");
                        // Deserialize the store partials message
                        roundNum = request.getInt();

                        len = request.getInt();
                        clientId = new byte[len];
                        request.get(clientId);

                        if (roundNum != getCurRound()) {
                            //handleRoundMismatch(new String(clientId));
                            handleRoundMismatch(new String(clientId),MessageCodes.STORE_PARTIAL);
                            continue;
                        }

                        partition = request.getInt();

                        len = request.getInt();
                        hash = new byte[len];
                        request.get(hash);

                        int replicaNum = request.getInt();
                        List<byte[]> replicas = new ArrayList<>();
                        byte[] replId;

                        for (int i = 0; i < replicaNum; i++) {
                            len = request.getInt();
                            replId = new byte[len];
                            request.get(replId);
                            replicas.add(replId);
                        }

                        storePartials(clientId, replicas, partition, hash);
                        //sendStoreReply(new String(clientId), MessageCodes.STORE_PARTIAL_REPLY,
                        //        "SENT STORE PARTIALS REPLY");
                        sendStoreReply(new String(clientId), MessageCodes.STORE_PARTIAL ,MessageCodes.STORE_PARTIAL_REPLY,
                                "SENT STORE PARTIALS REPLY");

                        break;
                    case MessageCodes.GET_PARTIAL:
                        // Deserialize the GET-PARTIALS message
                        roundNum = request.getInt();

                        len = request.getInt();
                        clientId = new byte[len];
                        request.get(clientId);

                        if (roundNum != getCurRound()) {
                            //handleRoundMismatch(new String(clientId));
                            handleRoundMismatch(new String(clientId),MessageCodes.GET_PARTIAL);
                            continue;
                        }

                        partition = request.getInt();

                        List<Pair<String, String>> partials = getPartials
                                (new Pair<>(new String(clientId), partition));
                        //sendGetPartialsReply(new String(clientId), partials, partition);
                        sendGetPartialsReply(new String(clientId), partials, partition, MessageCodes.GET_PARTIAL);

                        break;
                    default:
                        break;
                }
            } while (true);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}