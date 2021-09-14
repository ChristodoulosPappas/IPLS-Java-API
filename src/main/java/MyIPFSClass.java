import io.ipfs.api.*;
import io.ipfs.multihash.Multihash;
import org.javatuples.*;
import org.javatuples.Pair;
import org.web3j.abi.datatypes.Int;
import org.web3j.abi.datatypes.Type;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.Period;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyIPFSClass {
    public static IPFS ipfsObj;
    public static Semaphore mtx = new Semaphore(1);

    public MyIPFSClass(String Path) {
        ipfsObj = new IPFS(Path);
    }
    public MyIPFSClass(String Path, int timeout) {
        ipfsObj = new IPFS(Path);
        ipfsObj.timeout(timeout);
    }
    public MyIPFSClass(){}


    public static void initialize_IPLS_directory() throws IOException {
        String dirname = "IPLS_directory_" + PeerData._ID;

        boolean rval;
        System.out.println("Creating directory and essential files");
        System.out.println(PeerData._ID);
        System.out.println(PeerData._ID.length());
        File dir = new File(dirname);
        File file;
        System.out.println(dir.mkdir());

        file = new File(dirname + "/Auxiliaries");
        file.createNewFile();
        file = new File(dirname + "/State");
        file.createNewFile();
        file = new File(dirname + "/Participants");
        file.createNewFile();
        for(int i = 0; i < PeerData._PARTITIONS; i++){
            file = new File(dirname + "/" + i + "_Gradients");
            file.createNewFile();
            file = new File(dirname + "/" + i + "_Replicas");
            file.createNewFile();
            file = new File(dirname + "/" + i + "_Updates");
            file.createNewFile();

        }

    }


    public static Multihash add_file(String Filename) throws IOException {
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        //System.out.println(Filename);

        NamedStreamable.FileWrapper file = new NamedStreamable.FileWrapper(new File(Filename));
        //System.out.println(file);

        List<MerkleNode> node = ipfsObj.add(file);
        return node.get(node.size()-1).hash;
    }

    public static void Update_file(String filename, List<?> Weights,SecretKey key) throws Exception {
        byte[] bytes = encrypt(Weights,key);
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(bytes);
        oos.close();
        fos.close();
        bytes = null;
    }

    public static  void Update_file(String filename, List<?> Weights) throws Exception {
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(Weights);
        oos.close();
        fos.close();
    }
    
    public static void Update_file(String filename, Map<Integer,Integer> Participants) throws Exception {
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(Participants);
        oos.close();
        fos.close();
    }

    //Case state == 1, then we write a gradients file
    //Case state == 2, then we write only in the gradients the iteration number
    //Case state == 3, then we write also in the updates the iteration number
    public static void update_IPLS_directory(Map<Integer,List<Double>> Gradients, int state) throws Exception{
        List<Double> auxilary_list = new ArrayList<>();
        Map<Integer,Integer> Participants = new HashMap<>();
        String dir_name = "IPLS_directory_" + PeerData._ID + "/";
        if(state == 2){
            auxilary_list.add((double)PeerData.middleware_iteration);
            auxilary_list.add((double)PeerData.middleware_iteration-1);
            auxilary_list.add((double)(PeerData.middleware_iteration-1));
            Update_file(dir_name + "Auxiliaries",auxilary_list);
            for(int i = 0; i < PeerData._PARTITIONS; i++){
                if(!PeerData.Auth_List.contains(i)){
                    Update_file(dir_name + i +"_Gradients",Gradients.get(i));
                }
            }
        }
        else if(state == 3){
            auxilary_list.add((double)PeerData.middleware_iteration);
            auxilary_list.add((double)PeerData.middleware_iteration);
            auxilary_list.add((double)(PeerData.middleware_iteration-1));
            Update_file(dir_name + "Auxiliaries",auxilary_list);
            for(int i = 0; i < PeerData.Auth_List.size(); i++){
                Participants.put(PeerData.Auth_List.get(i),PeerData.workers.get(PeerData.Auth_List.get(i)).size());
                Update_file(dir_name + PeerData.Auth_List.get(i) +"_Replicas",Gradients.get(PeerData.Auth_List.get(i)));
            }
            Update_file(dir_name+"Participants",Participants);
        }
        else if(state == 4){
            auxilary_list.add((double)PeerData.middleware_iteration);
            auxilary_list.add((double)PeerData.middleware_iteration);
            auxilary_list.add((double)(PeerData.middleware_iteration));
            Update_file(dir_name + "Auxiliaries",auxilary_list);
            for(int i = 0; i < PeerData._PARTITIONS; i++){
                if(PeerData.Auth_List.contains(i)){
                    Update_file(dir_name + i +"_Updates",Gradients.get(i));
                }
            }
        }
    }


    public static void publish_gradients(Map<Integer,List<Double>> Gradients, int state) throws Exception {
        Multihash dir_hash;
        update_IPLS_directory(Gradients,state);
        dir_hash = add_file("IPLS_directory_" + PeerData._ID + "/");
        ipfsObj.name.publish(dir_hash);
    }


    public static String check_peer(String Peer) throws IOException {
        return ipfsObj.name.resolve(Peer);
    }


    public Multihash _Upload_File(List<?> Weights, String filename) throws IOException {
        //Serialize the Partition model into a file
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(Weights);
        oos.close();
        fos.close();
        // Add file into ipfs system
        return  add_file(filename);
    }

    public Multihash _Upload_File(Map<Integer,List<Double>> Weights, String filename) throws IOException {
        //Serialize the Partition model into a file
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(Weights);
        oos.close();
        fos.close();
        // Add file into ipfs system
        return  add_file(filename);
    }

    public boolean Download_Updates(String Hash){
        try {
            ipfsObj.get(Hash);
            return true;
        }
        catch(Exception e){
            System.out.println("time out");
            return false;
        }
    }

    public byte[] Get_bytes(String hash) throws Exception {
        byte[] bytes;
        ByteArrayInputStream bis = new ByteArrayInputStream(ipfsObj.cat(hash));
        ObjectInput in = new ObjectInputStream(bis);
        bytes = (byte[]) in.readObject();
        in.close();
        bis.close();
        return bytes;
    }

    public  List<?> DownloadParameters(String hash) throws IOException, ClassNotFoundException {
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        byte[] data;
        List<Double> Numerical_data = new ArrayList<>();
        //System.out.println(hash);
        data = ipfsObj.cat(hash);

        //System.out.println(data);
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        //System.out.println(bis);
        ObjectInput in = new ObjectInputStream(bis);
        Numerical_data = (List<Double>) in.readObject();
        in.close();
        bis.close();
        return Numerical_data;
    }

    static List<String> find_providers(String Hash) throws Exception{
        List<String> providers = new ArrayList<>();
        List<Map<String, Object>> rval = ipfsObj.dht.findprovs(Hash);
        for(int i = 0; i < rval.size(); i++){
            if(rval.get(i).get("Responses") != null && rval.get(i).get("Type").toString().equals("4") ){
                List<Map<String,Object>> arr = (List<Map<String, Object>>) rval.get(i).get("Responses");
                providers.add(arr.get(0).get("ID").toString());
            }
        }
        return providers;
    }

    static boolean has_providers(String Hash) throws Exception{
        List<String> providers = find_providers(Hash);
        for(int i = 0; i < providers.size(); i++){
            try {
                Map peers = ipfsObj.dht.findpeer(providers.get(i));
                if(peers.get("Responses") != null) {
                    return true;
                }
            }
            catch(Exception e){}

        }
        return false;
    }

    public Map<Integer,Integer> DownloadMap(String hash) throws Exception{
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        byte[] data;
        Map<Integer,Integer> Data = new HashMap<>();
        data = ipfsObj.cat(hash);

        //System.out.println(data);
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        //System.out.println(bis);
        ObjectInput in = new ObjectInputStream(bis);
        Data = (Map<Integer, Integer>) in.readObject();
        in.close();
        bis.close();
        return Data;

    }

    public static SecretKey Generate_key() throws Exception{
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");

        SecureRandom secureRandom = new SecureRandom();
        int keyBitSize = 256;
        keyGenerator.init(keyBitSize, secureRandom);
        return keyGenerator.generateKey();
    }

    public static byte[] encrypt(List<?> Arr,SecretKey key) throws Exception{
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE,key,new IvParameterSpec(new byte[16]));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(Arr);
        byte[] bytes = bos.toByteArray();
        byte[] encrypted_bytes;

        oos.close();
        bos.close();
        encrypted_bytes = cipher.doFinal(bytes);

        return encrypted_bytes;
    }

    public static List<?> decrypt(byte[] encrypted_data,SecretKey key) throws Exception{
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE,key,new IvParameterSpec(new byte[16]));
        byte[] bytes = cipher.doFinal(encrypted_data);
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        //System.out.println(bis);
        ObjectInput in = new ObjectInputStream(bis);
        List<Double> Numerical_data = (List<Double>) in.readObject();
        in.close();
        bis.close();
        return Numerical_data;
    }

    public List<?> Get_Message(String Peer,String Path) throws Exception{
        return DownloadParameters(check_peer(Peer) + "/" + Path);
    }
    public Map<Integer,Integer> Get_Participant_Number(String Peer,String Path) throws Exception{
        return DownloadMap(check_peer(Peer) + "/" + Path);
    }
    public  Map<Integer,List<Double>> DownloadMapParameters(String hash) throws IOException, ClassNotFoundException {
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        byte[] data;
        data = ipfsObj.cat(hash);
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = new ObjectInputStream(bis);
        return (Map<Integer,List<Double>>) in.readObject();
    }

    public void get_file(String name) throws IOException, ClassNotFoundException {
        System.out.println(this.DownloadParameters(name));
    }
    public static void get_content() throws IOException {
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        Map<Multihash, Object> mp = ipfsObj.pin.ls(IPFS.PinType.all);
        for (Map.Entry<Multihash, Object> hash : mp.entrySet()) {
            System.out.println(hash.getKey() + " , " + hash.getKey() + " , " + hash.getValue());
        }

    }

    public static KeyInfo create_key(String name) throws IOException {
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        KeyInfo key = ipfsObj.key.gen(name, Optional.empty(), Optional.empty());
        return key;

    }

    //Find in which global iteration the peer is based on its current schedule.
    // In case the time the peer cannot find the global iteration of the system -1 is returned
    public static int find_iter() throws Exception{
        mtx.acquire();

        for(int i = 0; i < (PeerData.current_schedule.size()-1)/4; i++){
            if(PeerData.current_schedule.get(4*i + 2 + 1) > (int) Instant.now().getEpochSecond()){
                mtx.release();
                return PeerData.current_schedule.get(4*i + 3 + 1);
            }

        }
        mtx.release();
        return -1;
    }

    // Get the maximum time needed for the peer to
    // train the model
    public static int get_training_time() throws Exception{
        if(find_iter() == -1){
            return -1;
        }
        return  PeerData.current_schedule.get(1) - PeerData.current_schedule.get(0);
    }

    // Get the maximum pure time needed that the peer is going to
    // use for the aggregation phase
    public static int get_aggregation_time() throws Exception{
        if(find_iter() == -1){
            return -1;
        }
        return  PeerData.current_schedule.get(2) - PeerData.current_schedule.get(1);
    }

    // Get the maximum pure time needed that the peer is going to
    // use for the synchronization phase
    public static int get_synch_time() throws Exception{
        if(find_iter() == -1){
            return -1;
        }
        return  PeerData.current_schedule.get(3) - PeerData.current_schedule.get(2);
    }

    // Time until training must be completed
    public static int training_elapse_time(int iteration){
        if(PeerData.current_schedule.size() == 0 || PeerData.current_schedule.get(PeerData.current_schedule.size()-1) < iteration){
            return -1;
        }
        int offset = iteration - PeerData.current_schedule.get(3+1);
        if(offset < 0){
            return -1;
        }
        return PeerData.current_schedule.get(offset*4 + 1);
    }

    // Time until aggregation must be completed
    public int aggregation_elapse_time(int iteration){
        if(PeerData.current_schedule.size() == 0 || PeerData.current_schedule.get(PeerData.current_schedule.size()-1) < iteration){
            return -1;
        }
        int offset = iteration - PeerData.current_schedule.get(3 + 1);
        if(offset < 0){
            return -1;
        }
        return PeerData.current_schedule.get(offset*4 + 1 + 1);
    }

    // Time until replicas synchronization must be completed
    public int synch_elapse_time(int iteration){
        if(PeerData.current_schedule.size() == 0 || PeerData.current_schedule.get(PeerData.current_schedule.size()-1) < iteration){
            return -1;
        }
        int offset = iteration - PeerData.current_schedule.get(3 + 1);
        if(offset < 0){
            return -1;
        }
        return PeerData.current_schedule.get(offset*4 + 2 + 1);
    }


    public int get_curr_time(){
        return (int) Instant.now().getEpochSecond();
    }

    public void clear_client_wait_ack_list () throws InterruptedException {
        if(PeerData.Client_Wait_Ack.size() > 0 && PeerData.Client_Wait_Ack.get(0).getValue2() <= PeerData.middleware_iteration){
            PeerData.Client_Wait_Ack = new ArrayList<>();
            System.out.println("CLEARING client_wait_ack list");
        }

    }


    public void clear_wait_ack_list () throws InterruptedException {
        if(PeerData.Wait_Ack.size() > 0 && PeerData.Wait_Ack.get(0).getValue2() <= PeerData.middleware_iteration){
            PeerData.Wait_Ack = new ArrayList<>();
            System.out.println("CLEARING WAIT ACK LIST! Time : " + get_curr_time() + " , next training time : " + training_elapse_time(PeerData.middleware_iteration+1) + " , " + PeerData.Wait_Ack);
        }
    }

    public void clear_replica_wait_ack_list() throws InterruptedException{

        if(PeerData.Replica_Wait_Ack.size() > 0 && PeerData.Replica_Wait_Ack.get(0).getValue2() <= PeerData.middleware_iteration){
            PeerData.Replica_Wait_Ack = new ArrayList<>();
        }

    }

    public void wait_next_iter() throws Exception{
        if(PeerData.current_schedule.size() == 0){
            return;
        }
        int iter = find_iter();
        int sleep_time = synch_elapse_time(iter) - get_curr_time();
        System.out.println(sleep_time + " , " + iter);
        while(sleep_time > 0){
            Thread.sleep(sleep_time*1000);
            sleep_time = synch_elapse_time(iter) - get_curr_time();
            System.out.println(sleep_time);
        }
        PeerData.middleware_iteration = find_iter();
    }

    public int get_number_of_commitments(){
        int commitments = 0;
        for(int i = 0; i < PeerData.Auth_List.size(); i++){
            commitments += PeerData.Committed_Hashes.get(PeerData.Auth_List.get(i)).size();
        }
        return commitments;
    }

    public void download_schedule(String schedule_hash) throws Exception, ClassNotFoundException {
        if(schedule_hash.equals("None")){
            return;
        }
        mtx.acquire();
        if(PeerData.current_schedule.size() == 0 || PeerData.current_schedule.get(0) < ((List<Integer>) DownloadParameters(schedule_hash)).get(0)){
            System.out.println("REPLACING SCHEDULE");
            PeerData.current_schedule = (List<Integer>) DownloadParameters(schedule_hash);
            PeerData.Schedule_Hash = schedule_hash;
            System.out.println("Schedule : " + PeerData.current_schedule + " , " + Instant.now().getEpochSecond());
        }
        mtx.release();
    }

    public static void publish_key(String name, int partition) throws IOException {
        KeyInfo key;
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        key = create_key(name);
        System.out.println(key);
        //System.out.println(ipfsObj.name.publish(key.id, Optional.of(key.name)));

    }

    // After we took the publish from a node we UnMarshall the String which is encoded in
    // ISO , with the form [BUFF_SIZE (INT), GRADIENT (DOUBLE),...,GRADIENT]
    public static double[] UnMarshall_Gradients(String returnVal) throws IOException, ClassNotFoundException {
        int i, arr_len;
        double d;
        byte[] bytes_array = Base64.getUrlDecoder().decode(returnVal);


        ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);


        arr_len = rbuff.getInt(0);
        double[] Gradients = new double[arr_len];
        for (i = 0; i < arr_len; i++) {
            d = rbuff.getDouble(i * Double.BYTES + Integer.BYTES);
            Gradients[i] = d;
        }
        return Gradients;
    }


    //We form the double array into a String in order to publish it with the form
    public static String Marshall_Gradients(double[] arr) throws IOException {
        int i;

        ByteBuffer buff = ByteBuffer.allocate(Double.BYTES * arr.length + Integer.BYTES);
        buff.putInt(0,arr.length);
        for (i = 0; i < arr.length; i++) {
            buff.putDouble(i * Double.BYTES + Integer.BYTES, arr[i]);
        }
        byte[] barr = new byte[buff.remaining()];
        buff.get(barr);


        return  Base64.getUrlEncoder().encodeToString(barr);
        //System.out.println(new String(buff.array(),"ISO-8859-1"));
        //return new String(buff.array(), "ISO-8859-1");
    }

    public static String Marshall_getAuth(int[] Auth_to_send){
        int i;
        ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES * (Auth_to_send.length+1) + 2* Short.BYTES);
        buff.putShort(0,(short) 1);
        buff.putShort(Short.BYTES,(short) 1);
        buff.putInt(2*Short.BYTES,Auth_to_send.length);
        for(i = 0; i < Auth_to_send.length; i++){
            buff.putInt(2*Short.BYTES + (i + 1)*Integer.BYTES,Auth_to_send[i]);
        }
        byte[] barr = new byte[buff.remaining()];
        buff.get(barr);

        return Base64.getUrlEncoder().encodeToString(barr);
    }

    public static String Marshall_Packet(List<Integer> Responsibilities,String Peer,String OriginPeer,short pid){
        int i;
        byte[] finalbarr;
        if(Responsibilities == null || Responsibilities.size() == 0){
            return null;
        }
        if(pid == 1) {
            ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES * (Responsibilities.size() + 1)+3*Short.BYTES);
            buff.putShort(0,pid);
            buff.putInt(Short.BYTES, Responsibilities.size());
            for (i = 0; i < Responsibilities.size(); i++) {
                buff.putInt((i + 1) * Integer.BYTES + Short.BYTES, Responsibilities.get(i));
            }
            buff.putShort((i + 1) * Integer.BYTES + Short.BYTES,(short)Peer.length());
            buff.putShort((i + 1) * Integer.BYTES + 2*Short.BYTES,(short)OriginPeer.length());



            byte[] barr = new byte[buff.remaining()];
            byte[] idBytes = Peer.getBytes();
            byte[] OriginIdBytes = OriginPeer.getBytes();
            System.out.println(OriginIdBytes.length);
            buff.get(barr);
            finalbarr = new byte[barr.length + idBytes.length + OriginIdBytes.length];
            for (i = 0; i < barr.length; i++) {
                finalbarr[i] = barr[i];
            }
            for (i = barr.length; i < barr.length + idBytes.length; i++) {
                finalbarr[i] = idBytes[i - barr.length];
            }
            for (i = barr.length + Peer.length(); i < finalbarr.length; i++){
                finalbarr[i] = OriginIdBytes[i - barr.length - Peer.length()];
            }

        }
        else{
            ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES * (Responsibilities.size() +1)+ Short.BYTES );
            buff.putShort(0,pid);
            buff.putInt(Short.BYTES, Responsibilities.size());
            for(i = 0; i < Responsibilities.size(); i++){
                buff.putInt((i+1)*Integer.BYTES + Short.BYTES ,Responsibilities.get(i));
            }
            byte[] barr = new byte[buff.remaining()];
            byte[] OriginIdBytes = OriginPeer.getBytes();

            buff.get(barr);
            finalbarr = new byte[barr.length + OriginPeer.length()];
            for (i = 0; i < barr.length; i++) {
                finalbarr[i] = barr[i];
            }
            for (i = barr.length ; i < finalbarr.length; i++){
                finalbarr[i] = OriginIdBytes[i - barr.length];
            }

        }

        return Base64.getUrlEncoder().encodeToString(finalbarr);
    }

    public static String Marshall_Packet(List<Double> Responsibilities,String OriginPeer,int Partition,short pid){
        int i;
        byte[] finalbarr;

        ByteBuffer buff = ByteBuffer.allocate(Double.BYTES * (Responsibilities.size()) + 2*Integer.BYTES+ Short.BYTES );
        buff.putShort(0,pid);
        buff.putInt(Short.BYTES, Responsibilities.size());
        buff.putInt(Short.BYTES+Integer.BYTES,Partition);
        for(i = 0; i < Responsibilities.size(); i++){
            buff.putDouble(i*Double.BYTES  + 2*Integer.BYTES + Short.BYTES ,Responsibilities.get(i));
        }
        byte[] barr = new byte[buff.remaining()];
        byte[] OriginIdBytes = OriginPeer.getBytes();

        buff.get(barr);
        finalbarr = new byte[barr.length + OriginPeer.length()];
        for (i = 0; i < barr.length; i++) {
            finalbarr[i] = barr[i];
        }
        for (i = barr.length ; i < finalbarr.length; i++){
            finalbarr[i] = OriginIdBytes[i - barr.length];
        }

        return Base64.getUrlEncoder().encodeToString(finalbarr);
    }

    public static String Marshall_Packet(List<Double> Responsibilities,String OriginPeer,int Partition,int iteration,short pid){
        int i,responsibilities_size;
        byte[] finalbarr;

        responsibilities_size = (Responsibilities == null)?0:Responsibilities.size();

        ByteBuffer buff = ByteBuffer.allocate(Double.BYTES * (responsibilities_size) + 3*Integer.BYTES+ Short.BYTES );
        buff.putShort(0,pid);
        buff.putInt(Short.BYTES, responsibilities_size);
        buff.putInt(Short.BYTES+Integer.BYTES,Partition);
        buff.putInt(Short.BYTES+2*Integer.BYTES,iteration);
        for(i = 0; i < responsibilities_size; i++){
            buff.putDouble(i*Double.BYTES  + 3*Integer.BYTES + Short.BYTES ,Responsibilities.get(i));
        }
        byte[] barr = new byte[buff.remaining()];
        byte[] OriginIdBytes = OriginPeer.getBytes();

        buff.get(barr);
        finalbarr = new byte[barr.length + OriginPeer.length()];
        for (i = 0; i < barr.length; i++) {
            finalbarr[i] = barr[i];
        }
        for (i = barr.length ; i < finalbarr.length; i++){
            finalbarr[i] = OriginIdBytes[i - barr.length];
        }

        return Base64.getUrlEncoder().encodeToString(finalbarr);
    }

    public static String Marshall_Packet(String OriginPeer,short pid){
        int i,size;
        byte[] finalbarr;

        size = Short.BYTES;
        if(pid == 6){
            size += Integer.BYTES;
        }

        ByteBuffer buff = ByteBuffer.allocate( size);
        buff.putShort(0,pid);
        if(pid == 6){
            buff.putInt(Short.BYTES,PeerData._Iter_Clock);
        }
        byte[] barr = new byte[buff.remaining()];
        byte[] OriginIdBytes = OriginPeer.getBytes();

        buff.get(barr);
        finalbarr = new byte[barr.length + OriginPeer.length()];
        for (i = 0; i < barr.length; i++) {
            finalbarr[i] = barr[i];
        }
        for (i = barr.length ; i < finalbarr.length; i++){
            finalbarr[i] = OriginIdBytes[i - barr.length];
        }
        return Base64.getUrlEncoder().encodeToString(finalbarr);
    }

    public static String Marshall_Packet(List<String> Peers,boolean isReply){
        int counter = 0,data_size = 0;
        byte[] finalbarr;

        ByteBuffer buff = ByteBuffer.allocate(Short.BYTES*(Peers.size() + 3));

        buff.putShort(0,(short) 7);
        if(isReply){
            buff.putShort(Short.BYTES,(short) 1);
        }
        else{
            buff.putShort(Short.BYTES,(short) 0);
        }
        buff.putShort(Short.BYTES*2,(short) Peers.size());
        for(int i = 0; i < Peers.size(); i++){
            buff.putShort(Short.BYTES*(3+i),(short)Peers.get(i).length());
            data_size += Peers.get(i).length();
        }
        byte[] barr = new byte[buff.remaining()];
        buff.get(barr);
        finalbarr = new byte[barr.length + data_size];
        for(counter = 0; counter <  barr.length; counter++){
            finalbarr[counter] = barr[counter];
        }
        byte[] String_Bytes;
        for(int i = 0; i < Peers.size(); i++){
            String_Bytes = Peers.get(i).getBytes();
            for(int j = 0; j < Peers.get(i).length(); j++){
                finalbarr[counter] = String_Bytes[j];
                counter++;
            }
        }

        return Base64.getUrlEncoder().encodeToString(finalbarr);
    }

    public static String Marshall_Packet(String Hash, String OriginPeer,int Partition,short pid) {
        int i;
        byte[] finalbarr;

        ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES + 3*Short.BYTES);
        buff.putShort(0,pid);
        buff.putShort(Short.BYTES,(short) Hash.length());
        buff.putShort(2*Short.BYTES,(short) OriginPeer.length());
        buff.putInt(3*Short.BYTES,Partition);
        byte[] barr = new byte[buff.remaining()];
        byte[] HashBytes = Hash.getBytes();
        byte[] OriginIdBytes = OriginPeer.getBytes();
        buff.get(barr);
        finalbarr = new byte[barr.length + Hash.length() + OriginPeer.length()];
        for (i = 0; i < barr.length; i++) {
            finalbarr[i] = barr[i];
        }
        for (i = barr.length; i < barr.length + Hash.length(); i++) {
            finalbarr[i] = HashBytes[i - barr.length];
        }
        for (i = barr.length + Hash.length(); i < finalbarr.length; i++){
            finalbarr[i] = OriginIdBytes[i - barr.length - Hash.length()];
        }

        return Base64.getUrlEncoder().encodeToString(finalbarr);
    }

    public static String Marshall_Packet(String Hash, String OriginPeer,int Partition,int iteration,short pid) {
        int i;
        byte[] finalbarr;

        ByteBuffer buff = ByteBuffer.allocate(2*Integer.BYTES + 3*Short.BYTES);
        buff.putShort(0,pid);
        buff.putShort(Short.BYTES,(short) Hash.length());
        buff.putShort(2*Short.BYTES,(short) OriginPeer.length());
        buff.putInt(3*Short.BYTES,Partition);
        buff.putInt(3*Short.BYTES + Integer.BYTES,iteration);

        byte[] barr = new byte[buff.remaining()];
        byte[] HashBytes = Hash.getBytes();
        byte[] OriginIdBytes = OriginPeer.getBytes();
        buff.get(barr);
        finalbarr = new byte[barr.length + Hash.length() + OriginPeer.length()];
        for (i = 0; i < barr.length; i++) {
            finalbarr[i] = barr[i];
        }
        for (i = barr.length; i < barr.length + Hash.length(); i++) {
            finalbarr[i] = HashBytes[i - barr.length];
        }
        for (i = barr.length + Hash.length(); i < finalbarr.length; i++){
            finalbarr[i] = OriginIdBytes[i - barr.length - Hash.length()];
        }

        return Base64.getUrlEncoder().encodeToString(finalbarr);
    }
    public static String Marshall_Packet(String OriginPeer, String Hash, int Partition, int iteration, SecretKey key, short pid) throws Exception{
        byte[] key_bytes;
        byte[] finalbarr;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(key);
        key_bytes = bos.toByteArray();


        ByteBuffer buff = ByteBuffer.allocate(3*Integer.BYTES + 3*Short.BYTES);
        buff.putShort(0,pid);
        buff.putShort(Short.BYTES,(short) Hash.length());
        buff.putShort(2*Short.BYTES,(short) OriginPeer.length());
        buff.putInt(3*Short.BYTES,Partition);
        buff.putInt(3*Short.BYTES + Integer.BYTES,iteration);
        buff.putInt(3*Short.BYTES + 2*Integer.BYTES,key_bytes.length);

        byte[] barr = new byte[buff.remaining()];
        byte[] HashBytes = Hash.getBytes();
        byte[] OriginIdBytes = OriginPeer.getBytes();
        buff.get(barr);
        finalbarr = new byte[barr.length + Hash.length() + OriginPeer.length() + key_bytes.length];
        for (int i = 0; i < barr.length; i++) {
            finalbarr[i] = barr[i];
        }
        for (int i = barr.length; i < barr.length + Hash.length(); i++) {
            finalbarr[i] = HashBytes[i - barr.length];
        }
        for (int i = barr.length + Hash.length(); i < barr.length + Hash.length() + OriginPeer.length(); i++){
            finalbarr[i] = OriginIdBytes[i - barr.length - Hash.length()];
        }

        for (int i = barr.length+ Hash.length() + OriginPeer.length(); i < finalbarr.length; i++){
            finalbarr[i] = key_bytes[i - (barr.length+ Hash.length() + OriginPeer.length())];
        }
        return Base64.getUrlEncoder().encodeToString(finalbarr);
    }

    public static String Marshall_Packet(List<Integer> Auth,List<String> Peers){
        ByteBuffer buff = ByteBuffer.allocate((Auth.size())*Integer.BYTES  + (2 + Peers.size())*Short.BYTES);
        byte[] finalbarr;
        int counter = 0;
        int data_size = 0;

        buff.putShort(0,(short) 11);
        buff.putShort(Short.BYTES,(short)Auth.size());
        for(int i = 0; i < Auth.size(); i++){
            buff.putInt(2*Short.BYTES + i*Integer.BYTES, Auth.get(i));
        }
        for(int i = 0; i < Auth.size()+2; i++){
            buff.putShort((2+i)*Short.BYTES + Auth.size()*Integer.BYTES , (short)Peers.get(i).length());
            data_size += Peers.get(i).length();
        }
        byte[] barr = new byte[buff.remaining()];
        buff.get(barr);
        finalbarr = new byte[barr.length + data_size];
        for(counter = 0; counter <  barr.length; counter++){
            finalbarr[counter] = barr[counter];
        }
        byte[] String_Bytes;
        for(int i = 0; i < Peers.size(); i++){
            String_Bytes = Peers.get(i).getBytes();
            for(int j = 0; j < Peers.get(i).length(); j++){
                finalbarr[counter] = String_Bytes[j];
                counter++;
            }
        }

        return Base64.getUrlEncoder().encodeToString(finalbarr);

    }


    public static String JOIN_PACKET(String Peer,int Partition,int iteration,short reply){
        ByteBuffer buff = ByteBuffer.allocate(2*Integer.BYTES  +  2*Short.BYTES);
        byte[] finalbarr;
        buff.putShort(0,(short) 8);
        buff.putShort(Short.BYTES,reply);
        buff.putInt(2*Short.BYTES, Partition);
        buff.putInt(2*Short.BYTES + Integer.BYTES,iteration);
        byte[] barr = new byte[buff.remaining()];
        buff.get(barr);
        finalbarr = new byte[barr.length + Peer.length()];

        byte[] PeerId = Peer.getBytes();

        for (int i = 0; i < barr.length; i++) {
            finalbarr[i] = barr[i];
        }
        for (int i = barr.length ; i < finalbarr.length; i++){
            finalbarr[i] = PeerId[i - barr.length];
        }
        return Base64.getUrlEncoder().encodeToString(finalbarr);

    }

    public static String JOIN_PARTITION_SERVER(String Peer,int Partition,short reply){

        return JOIN_PACKET(Peer,Partition,PeerData.middleware_iteration,reply);

    }

    public static String _START_TRAINING(){
        ByteBuffer buff = ByteBuffer.allocate(Short.BYTES);
        byte[] finalbarr;
        buff.putShort(0,(short) 9);

        byte[] barr = new byte[buff.remaining()];
        buff.get(barr);
        finalbarr = new byte[barr.length + PeerData._ID.length()];

        byte[] PeerId = PeerData._ID.getBytes();

        for (int i = 0; i < barr.length; i++) {
            finalbarr[i] = barr[i];
        }
        for (int i = barr.length ; i < finalbarr.length; i++){
            finalbarr[i] = PeerId[i - barr.length];
        }
        return Base64.getUrlEncoder().encodeToString(finalbarr);
    }

    public static  Map<String,List<Integer>> Unmarshall_Peer_Responsibilities(String returnval) throws UnsupportedEncodingException {
        int i, arr_len;
        byte[] bytes_array = Base64.getUrlDecoder().decode(returnval);
        List<Integer> Responsibilities = new ArrayList<Integer>();
        Map<String,List<Integer>> rval = new HashMap<String,List<Integer>>();
        ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);

        arr_len = rbuff.getInt(0);
        byte[] Id_array = new byte[bytes_array.length - (arr_len+1)*Integer.BYTES];
        for (i = 0; i < arr_len; i++) {
            Responsibilities.add(rbuff.getInt((i+1)*Integer.BYTES));
        }
        for(i = (arr_len+1)*Integer.BYTES; i < bytes_array.length; i++){
            Id_array[i-(arr_len+1)*Integer.BYTES] = bytes_array[i];
        }
        String ID = returnval.substring((arr_len+1)*Integer.BYTES);

        rval.put(new String(Id_array),Responsibilities);

        return  rval;

    }

    public Map<String,List<Integer>> Get_Partitions(ByteBuffer rbuff,  byte[] bytes_array){
        int arr_len,i;
        String PeerId;
        arr_len = rbuff.getInt();
        Map<String,List<Integer>> map = new HashMap<String,List<Integer>>();
        List<Integer> Peer_Auth = new ArrayList<Integer>();
        for(i = 0; i < arr_len; i++){
            Peer_Auth.add(rbuff.getInt());
        }
        byte[] Id_array = new byte[bytes_array.length - (arr_len + 1) * Integer.BYTES - Short.BYTES];
        for (i = (arr_len + 1) * Integer.BYTES + Short.BYTES; i < bytes_array.length; i++) {
            Id_array[i - (arr_len + 1) * Integer.BYTES - Short.BYTES] = bytes_array[i];
        }
        System.out.println(Peer_Auth);

        PeerId = new String(Id_array);
        map.put(PeerId,Peer_Auth);
        return map;
    }

    //List<Double> is going to become INDArray one day
    // Deserializing Update message
    public Triplet<String,Integer,List<Double>> Get_Gradients(ByteBuffer rbuff, byte[] bytes_array){
        int arr_len,i,Partition;
        String PeerId;
        arr_len = rbuff.getInt();
        Partition = rbuff.getInt();
        List<Double> Gradients = new ArrayList<Double>();

        for(i = 0; i < arr_len; i++){
            Gradients.add(rbuff.getDouble());
        }
        byte[] Id_array = new byte[bytes_array.length - arr_len*Double.BYTES - 2 *Integer.BYTES - Short.BYTES];
        for (i = arr_len * Double.BYTES + 2 * Integer.BYTES + Short.BYTES; i < bytes_array.length; i++) {
            Id_array[i - arr_len * Double.BYTES - 2 * Integer.BYTES - Short.BYTES] = bytes_array[i];
        }
        //System.out.println(Gradients);

        PeerId = new String(Id_array);

        return new Triplet<>(PeerId,Partition,Gradients);
    }

    public Quartet<String,Integer,Integer,List<Double>> GET_GRADIENTS(ByteBuffer rbuff, byte[] bytes_array){
        int arr_len,i,Partition,iteration;
        String PeerId;
        arr_len = rbuff.getInt();
        Partition = rbuff.getInt();
        iteration = rbuff.getInt();
        List<Double> Gradients = new ArrayList<Double>();

        for(i = 0; i < arr_len; i++){
            Gradients.add(rbuff.getDouble());
        }
        if(arr_len == 0){
            Gradients = null;
        }
        byte[] Id_array = new byte[bytes_array.length - arr_len*Double.BYTES - 3 *Integer.BYTES - Short.BYTES];
        for (i = arr_len * Double.BYTES + 3 * Integer.BYTES + Short.BYTES; i < bytes_array.length; i++) {
            Id_array[i - arr_len * Double.BYTES - 3 * Integer.BYTES - Short.BYTES] = bytes_array[i];
        }
        //System.out.println(Gradients);

        PeerId = new String(Id_array);

        return new Quartet<>(PeerId,Partition,iteration,Gradients);
    }
    //List<Double> is going to become INDArray one day
    // Deserializing Update message
    public Quartet<String,Integer,Integer,List<Double>> Get_Replica_Model(ByteBuffer rbuff, byte[] bytes_array){
        int arr_len,i,iteration,Peer_num;
        String PeerId;
        arr_len = rbuff.getInt();
        iteration = rbuff.getInt();
        Peer_num = rbuff.getInt();
        List<Double> Gradients = new ArrayList<Double>();

        for(i = 0; i < arr_len; i++){
            Gradients.add(rbuff.getDouble());
        }
        byte[] Id_array = new byte[bytes_array.length - arr_len*Double.BYTES - 3 *Integer.BYTES - Short.BYTES];
        for (i = arr_len * Double.BYTES + 3 * Integer.BYTES + Short.BYTES; i < bytes_array.length; i++) {
            Id_array[i - arr_len * Double.BYTES - 3 * Integer.BYTES - Short.BYTES] = bytes_array[i];
        }
        //System.out.println(Gradients);

        PeerId = new String(Id_array);

        return new Quartet<>(PeerId,iteration,Peer_num,Gradients);
    }

    public String Get_Peer(ByteBuffer rbuff,  byte[] bytes_array, int start){
        int i;
        byte[] Id_array = new byte[bytes_array.length  - start];

        for (i = start; i < bytes_array.length; i++) {
            Id_array[i - start] = bytes_array[i];
        }
        return  new String(Id_array);
    }


    public static List<String> Get_MultiaddrPeers(ByteBuffer rbuff, byte[] bytes_array){
        int list_size = 0,counter = 0;
        List<Short> sizes = new ArrayList<>();
        List<String> Peers = new ArrayList<>();
        list_size = rbuff.getShort();
        for(int i = 0; i < list_size; i++){
            sizes.add(rbuff.getShort());
        }

        counter = (list_size+3)*Short.BYTES;

        for(int i = 0; i < list_size; i++){
            byte[] StringBytes = new byte[sizes.get(i)];
            for(int j = 0; j < sizes.get(i); j++){
                StringBytes[j] = bytes_array[counter];
                counter++;
            }
            Peers.add(new String(StringBytes));
        }

        return Peers;
    }

    public Map<Integer,String> Get_RMap(ByteBuffer rbuff,  byte[] bytes_array){
        Map<Integer,String> Responsibilities = new HashMap<>();
        List<Integer> Auth = new ArrayList<>();
        List<Short> Peer_Lengths = new ArrayList<>();
        List<String> Peers = new ArrayList<>();
        int size = 0;
        int counter= 0;

        size = rbuff.getShort();
        System.out.println(size);
        for(int i = 0; i < size; i++){
            Auth.add(rbuff.getInt());
        }
        for(int i = 0; i < size+2; i++){
            Peer_Lengths.add(rbuff.getShort());
        }
        System.out.println(Peer_Lengths);

        counter = (size+4)*Short.BYTES + size*Integer.BYTES;

        for(int i = 0; i < size+2; i++){
            byte[] StringBytes = new byte[Peer_Lengths.get(i)];
            for(int j = 0; j < Peer_Lengths.get(i); j++){
                StringBytes[j] = bytes_array[counter];
                counter++;
            }
            Peers.add(new String(StringBytes));
        }
        System.out.println(Peers);
        for(int i = 0; i < size; i++){
            Responsibilities.put(Auth.get(i),Peers.get(i));
        }
        Responsibilities.put(-1,Peers.get(size));
        Responsibilities.put(-2,Peers.get(size+1));
        return Responsibilities;
    }

    public Triplet<Integer,String,String> Get_data_hash(ByteBuffer rbuff,  byte[] bytes_array){
        int Partition,i;
        short Hashsize,OriginPeerSize;
        String Hash,Origin_Peer;
        Hashsize = rbuff.getShort();
        OriginPeerSize = rbuff.getShort();
        Partition = rbuff.getInt();

        byte[] Id_array = new byte[bytes_array.length -  Integer.BYTES - 3*Short.BYTES - OriginPeerSize];
        byte[] Origin_array = new byte[bytes_array.length-  Integer.BYTES - 3*Short.BYTES - Hashsize];


        for (i =  Integer.BYTES + 3*Short.BYTES; i < Integer.BYTES + 3*Short.BYTES + Hashsize; i++) {
            Id_array[i -  Integer.BYTES - 3*Short.BYTES] = bytes_array[i];
        }
        for (i = Integer.BYTES + 3*Short.BYTES + Hashsize; i < bytes_array.length; i++){
            Origin_array[i - Integer.BYTES - 3*Short.BYTES - Hashsize] = bytes_array[i];
        }

        Hash = new String(Id_array);
        Origin_Peer = new String(Origin_array);
        return new Triplet<>(Partition,Hash,Origin_Peer);
    }

    public Quartet<Integer,Integer,String,String> Get_Commitment(ByteBuffer rbuff,  byte[] bytes_array){
        int Partition,iter,i;
        short Hashsize,OriginPeerSize;
        String Hash,Origin_Peer;
        Hashsize = rbuff.getShort();
        OriginPeerSize = rbuff.getShort();
        Partition = rbuff.getInt();
        iter = rbuff.getInt();

        byte[] Id_array = new byte[bytes_array.length -  2*Integer.BYTES - 3*Short.BYTES - OriginPeerSize];
        byte[] Origin_array = new byte[bytes_array.length-  2*Integer.BYTES - 3*Short.BYTES - Hashsize];


        for (i =  2*Integer.BYTES + 3*Short.BYTES; i < 2*Integer.BYTES + 3*Short.BYTES + Hashsize; i++) {
            Id_array[i -  2*Integer.BYTES - 3*Short.BYTES] = bytes_array[i];
        }
        for (i = 2*Integer.BYTES + 3*Short.BYTES + Hashsize; i < bytes_array.length; i++){
            Origin_array[i - 2*Integer.BYTES - 3*Short.BYTES - Hashsize] = bytes_array[i];
        }

        Hash = new String(Id_array);
        Origin_Peer = new String(Origin_array);
        return new Quartet<>(Partition,iter,Hash,Origin_Peer);
    }

    public Quintet<Integer,Integer,String,String,SecretKey> Get_SecretKey(ByteBuffer rbuff, byte[] bytes_array) throws Exception{
        int Partition,iter,key_size,i;
        short Hashsize,OriginPeerSize;
        String Hash,Origin_Peer;
        Hashsize = rbuff.getShort();
        OriginPeerSize = rbuff.getShort();
        Partition = rbuff.getInt();
        iter = rbuff.getInt();
        key_size = rbuff.getInt();

        byte[] Id_array = new byte[Hashsize];
        byte[] Origin_array = new byte[OriginPeerSize];
        byte[] Key_array = new byte[key_size];

        for (i =  3*Integer.BYTES + 3*Short.BYTES; i < 3*Integer.BYTES + 3*Short.BYTES + Hashsize; i++) {
            Id_array[i -  3*Integer.BYTES - 3*Short.BYTES] = bytes_array[i];
        }
        for (i = 3*Integer.BYTES + 3*Short.BYTES + Hashsize; i < 3*Integer.BYTES + 3*Short.BYTES + Hashsize + OriginPeerSize; i++){
            Origin_array[i - 3*Integer.BYTES - 3*Short.BYTES - Hashsize] = bytes_array[i];
        }
        for (i =  3*Integer.BYTES + 3*Short.BYTES + Hashsize + OriginPeerSize; i <  3*Integer.BYTES + 3*Short.BYTES + Hashsize + OriginPeerSize + key_size; i++){
            Key_array[i - (3*Integer.BYTES + 3*Short.BYTES + Hashsize + OriginPeerSize)] = bytes_array[i];
        }

        Hash = new String(Id_array);
        Origin_Peer = new String(Origin_array);
        ByteArrayInputStream bis = new ByteArrayInputStream(Key_array);
        //System.out.println(bis);
        ObjectInput in = new ObjectInputStream(bis);
        SecretKey key = (SecretKey) in.readObject();

        return new Quintet<>(Partition,iter,Hash,Origin_Peer,key);
    }

    public Triplet<String,Integer,Integer> Get_JoinRequest(ByteBuffer rbuff, byte[] bytes_array){
        byte[] Id_array = new byte[bytes_array.length  - 2*Short.BYTES - 2*Integer.BYTES];
        int Partition = rbuff.getInt();
        int iteration = rbuff.getInt();
        for (int i = 2*Short.BYTES + 2*Integer.BYTES; i < bytes_array.length; i++) {
            Id_array[i - 2*Short.BYTES - 2*Integer.BYTES] = bytes_array[i];
        }
        return new org.javatuples.Triplet<>(new String(Id_array),Partition,iteration);
    }


    /*
    // Here we wait for a publish from a specific topic to get gradients
    public double[] recv(IPFS ipfs,String topic) throws Exception {

        byte[] decodedBytes;
        String decodedString = null;
        Stream<Map<String, Object>> sub = ipfs.pubsub.sub(topic);
        System.out.println("OK");
        List<Map> results = sub.limit(1).collect(Collectors.toList());
        String encoded = (String) results.get(0).get("data");
        System.out.println(results);
        decodedBytes = Base64.getUrlDecoder().decode(encoded);
        decodedString = new String(decodedBytes);
        //System.out.println(Arrays.toString(UnMarshall_Gradients(decodedString)));
        System.out.println("Received");

        sub.close();

        return UnMarshall_Gradients(decodedString);
    }

    //In this function we publish the gradients
    public static void publish_gradients(String topic, double[] gradients) throws Exception {
        IPFS ipfs = new IPFS("/ip4/127.0.0.1/tcp/5001");
        String data = Marshall_Gradients(gradients);
        System.out.println(data);
        //System.out.println(data.toCharArray());
        Object pub = ipfs.pubsub.pub(topic,data);

    }

     */
}
