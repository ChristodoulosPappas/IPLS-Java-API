import io.ipfs.api.IPFS;
import io.ipfs.api.KeyInfo;
import io.ipfs.api.MerkleNode;
import io.ipfs.api.NamedStreamable;
import io.ipfs.multihash.Multihash;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyIPFSClass {
    public static IPFS ipfsObj;

    public MyIPFSClass(String Path) {
        ipfsObj = new IPFS(Path);
    }

    public static Multihash add_file(String Filename) throws IOException {
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        NamedStreamable.FileWrapper file = new NamedStreamable.FileWrapper(new File(Filename));
        List<MerkleNode> node = ipfsObj.add(file);
        System.out.println(node.get(0).name.get() + " Uploaded " + node.get(0).hash.toString());
        return node.get(0).hash;
    }

    public static List<Double> DownloadParameters(String hash) throws IOException, ClassNotFoundException {
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        byte[] data;
        Multihash rhash = Multihash.fromBase58(hash);
        data = ipfsObj.cat(rhash);
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = new ObjectInputStream(bis);
        return (List<Double>) in.readObject();
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


    public static void publish_key(String name) throws IOException {
        KeyInfo key;
        //IPFS ipfsObj = new IPFS("/ip4/127.0.0.1/tcp/5001");
        key = create_key(name);
        System.out.println(key);
        System.out.println(ipfsObj.name.publish(key.id, Optional.of(key.name)));

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

    public static String Marshall_Packet(String Hash,String OriginPeer,int Partition,short pid) {
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
