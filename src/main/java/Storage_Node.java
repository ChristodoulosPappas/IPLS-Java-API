import io.ipfs.api.IPFS;
import io.ipfs.api.Sub;
import org.apache.commons.cli.*;
//import org.ipfsbox.battery.api.IPFSCluster;
import org.javatuples.Pair;
import org.json.JSONObject;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;


// This class contains the code for the functionalitys of a peer
// that is part of IPLS decentralized storage. For such a peer
// two processes must run locally. The one is the ipfs daemon
// and the other one is the ipfs-cluste-service which is used
// for coordination among other storage peers.

// This process receives gradients or updates from participants and aggregators
// respectively and is responsible for storing and pinning those data so that
// they can be available even if the participants and aggregators aren't

public class Storage_Node {
    public static int cluster_port;
    public static String ipfs_addr;
    public static IPFS ipfs;
    public static MyIPFSClass ipfsClass;
    //public static IPFSCluster ipfsCluster;

    public static void parse_arguments(String[] args){

        Options options = new Options();

        Option addr = new Option("addr", "addr",true,"The address the IPFS daemon API is listening");
        options.addOption(addr);

        Option port = new Option("p", "p",true,"The port the ipfs-cluster-service is listening");
        options.addOption(port);
        DefaultParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            cluster_port = new Integer(cmd.getOptionValue("p"));
            ipfs_addr = cmd.getOptionValue("addr");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }
    }

    public static String get_request(String request){
        JSONObject obj = new JSONObject(request);
        System.out.println(obj);
        String encoded = (String) obj.get("data");
        byte[] decodedBytes = Base64.getUrlDecoder().decode(encoded);
        return new String(decodedBytes);
    }


    // Using this method, the storage not saves locally the partitions of the model
    // and syncs them to the other peers in order to replicate them.
    public static void pin_files(Map<Integer,List<Double>> data, String PeerID) throws Exception{
        List<Integer> keys = new ArrayList<>(data.keySet());
        String dirname = "IPLS_directory_" + PeerData._ID;

        for(int i = 0; i < keys.size(); i++){
            String filename = dirname + "/Peer_" + PeerID + "_" + keys.get(i);
            File file = new File(filename);
            file.createNewFile();
            ipfsClass.Update_file(filename, data.get(keys.get(i)));
            String HashID = ipfsClass.add_file(filename).toString();
            //ipfsCluster.pins.add(HashID);
        }
    }

    
    public static void process(String request) throws Exception{
        request = get_request(request);
        byte[] bytes_array = Base64.getUrlDecoder().decode(request);
        ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);
        short pid = rbuff.getShort();
        if(pid == 1){
            Pair<String,Map<Integer,List<Double>>> Data = ipfsClass.Data_to_pin(rbuff,bytes_array);

        }
    }

    public static void main(String[] args) throws Exception{
        parse_arguments(args);
        BlockingQueue<String> queue = new LinkedBlockingDeque<>();
        Sub SUB = new Sub("IPLS_Storage",ipfs_addr,queue,true);
        SUB.start();
        ipfs = new IPFS(ipfs_addr);
        ipfsClass = new MyIPFSClass(ipfs_addr);
        //ipfsCluster = new IPFSCluster("127.0.0.1",cluster_port);
        PeerData._ID = ipfs.id().get("ID").toString();
        PeerData._PARTITIONS = 0;
        ipfsClass.initialize_IPLS_directory();

        while(true){
            process(queue.take());
        }
    }

}
