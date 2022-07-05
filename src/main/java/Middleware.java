import io.ipfs.api.IPFS;
import org.apache.commons.cli.*;
//import sun.nio.ch.sctp.PeerAddrChange;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Middleware{
    public static  short task;
    public static List<String> Bootstrappers;
    public static  int model_size;
    public static  List<Double> Updates = new ArrayList<>();
    public static String Path;
    public static String FileName;
    public static IPLS ipls;
    public static boolean is_bootstraper;
    public static int port;

    public static void parse_arguments(String[] args){

        Options options = new Options();

        Option arg_port = new Option("p", "port_number", true, "The port number the daemon will interact with the python API");
        arg_port.setRequired(true);
        options.addOption(arg_port);


        Option partitions = new Option("pa", "partitions", true, "The number of partitions you want to partition the model");
        partitions.setRequired(true);
        options.addOption(partitions);

        Option minimum_partitions = new Option("mp", "minimum_partitions", true, "The minimum number of partitions a peer required to be responsible for");
        minimum_partitions.setRequired(true);
        options.addOption(minimum_partitions);


        Option min_peers = new Option("n", "min_peers", true, "The minimum number of peers required to proceed to training phase");
        min_peers.setRequired(true);
        options.addOption(min_peers);

        Option indirect_communication = new Option("i", "indirect_communication", true, "Insert 1 if indirect communication is true");
        indirect_communication.setRequired(true);
        options.addOption(indirect_communication );


        Option training_time = new Option("training","training",true,"Insert the training time of the FL process");
        training_time.setRequired(true);
        options.addOption(training_time);

        Option partial_aggregation = new Option("aggr","partial_aggregation",true,"Insert 1 if aggregators perform partial aggregation");
        partial_aggregation.setRequired(true);
        options.addOption(partial_aggregation);

        Option IPNS = new Option("IPNS", "IPNS",true,"Provide Indirect communication, instead of using message passing protocols use IPFS file system capabilities");
        options.addOption(IPNS);

        Option asynchronous = new Option("async", "Async",true,"If is true then you turn the protocol in asynchronous mod where you do not have to wait for others to complete the iteration");
        options.addOption(asynchronous);

        DefaultParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            port = new Integer(cmd.getOptionValue("port_number"));
            System.out.println(port);
            PeerData._PARTITIONS  = new Integer(cmd.getOptionValue("partitions"));
            PeerData._MIN_PARTITIONS = new Integer(cmd.getOptionValue("minimum_partitions"));
            PeerData.Min_Members = new Integer(cmd.getOptionValue("min_peers"));
            int communication = new Integer(cmd.getOptionValue("indirect_communication"));
            PeerData.Training_time = new Integer(cmd.getOptionValue("training"));
            int aggregation = new Integer(cmd.getOptionValue("partial_aggregation"));
            if(communication > 0){
                PeerData.Indirect_Communication = true;
            }
            else{
                PeerData.Indirect_Communication = false;
            }
            if(aggregation > 0){
                PeerData.Partial_Aggregation = true;
            }
            else{
                PeerData.Partial_Aggregation = false;
            }
            if(cmd.getOptionValue("IPNS") != null){
                if(cmd.getOptionValue("IPNS").equals("true")){
                    PeerData.IPNS_Enable = true;
                }
            }
            if(cmd.getOptionValue("async") != null){
                if(cmd.getOptionValue("async").equals("true")){
                    PeerData.isSynchronous = false;
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static String get_string(int string_size,DataInputStream in) throws IOException{
        byte[] string_bytes = new byte[string_size];
        for(int i = 0; i < string_size; i++){
            string_bytes[i] = in.readByte();
        }
        return  new String(string_bytes);

    }

    public static void Deserialize(DataInputStream in,
                                   int size) throws IOException {


        Bootstrappers = new ArrayList<>();


        task = in.readShort();
        if(task == 1){
            short bootstraper = in.readShort();
            if(bootstraper == 0) {
                is_bootstraper = false;
            }
            else{
                is_bootstraper = true;
            }
            PeerData.isBootsraper = is_bootstraper;
            short bootstrapers = in.readShort();

            for(int i = 0; i < bootstrapers; i++){
                short address_size = in.readShort();

                Bootstrappers.add(get_string(address_size,in));
            }
            short path_size = in.readShort();

            Path = get_string(path_size,in);
            short fileName_size = in.readShort();
            FileName = get_string(fileName_size,in);
            model_size = in.readInt();
            for(int i = 0; i < model_size; i++){
                Updates.add(0.0);
            }

        }
        else if(task == 2){
            for(int i = 0; i < model_size; i++){
                Updates.set(i,in.readDouble());
            }
        }

    }

    public static void Serialize(DataOutputStream out,
                                 List<Double> updates) throws IOException {

        for(int i = 0; i < model_size; i++){
            out.writeDouble(updates.get(i));
        }
    }

    public static void Get_Task(int size, Socket clientSocket) throws IOException {
        List<Double> updated_model = new ArrayList<>();
        //PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
        DataInputStream in = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));
        Deserialize(in,size);
    }


    public static void Return_Global_model( List<Double> global_model, Socket clientSocket) throws IOException{
        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
        out.flush();
        Serialize(out,global_model);

        out.flush();
    }

    public static void Send_Ack(Socket clientSocket) throws IOException{
        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
        out.flush();
        out.writeChar('A');

        out.flush();
    }

    public static List<Double> Encode(List<Double> updates){
        List<Double> encoded_gradients = new ArrayList<>();
        for(int i = 0; i < updates.size(); i++){
            if(updates.get(i) > 10.0){
                encoded_gradients.add(10*Math.pow(10,12));
            }
            else if(updates.get(i) < -10.0){
                encoded_gradients.add(-10*Math.pow(10,12));
            }
            else{
                encoded_gradients.add(new Double(updates.get(i) * Math.pow(10,12)).doubleValue());
            }
        }
        return  encoded_gradients;
    }

    public static void main(String argv[]) {
        parse_arguments(argv);
        try {
            int _TRAINING_START = 0, _TRAINING_END = 0;
            PeerData._LOG.put("training", new ArrayList<>());
            ServerSocket serverSocket = new ServerSocket(port);
            Light_IPLS_Daemon ipls_daemon = null;

            while (true) {
                System.out.println("WAITTING");
                Socket clientSocket = serverSocket.accept();
                System.out.println("GOT MESSAGE");
                Get_Task(2, clientSocket);
                if (task == 1) {
                    System.out.println("CREATE A NEW IPLS INSTANCE : " + Path + " , " + FileName + " , " + Bootstrappers + " , " + is_bootstraper + " , " + model_size);
                    ipls = new IPLS(Path, FileName, Bootstrappers, is_bootstraper, model_size);
                    //IF I AM BOOTSTRAPER THEN DO NOT CONTINUE
                    if (is_bootstraper) {
                        ipls.init();
                        while (true) {
                        }
                    }
                    ipls_daemon = new Light_IPLS_Daemon(ipls);
                    ipls_daemon.start();
                    Send_Ack(clientSocket);
                    clientSocket.close();
                } else if (task == 2) {
                    _TRAINING_END = (int) Instant.now().getEpochSecond();
                    int num = _TRAINING_END - _TRAINING_START;
                    PeerData._LOG.get("training").add(num);
                    if(PeerData.secure_ipls){
                        ipls_daemon.UpdateModel(Encode(Updates));
                    }
                    else{
                        ipls_daemon.UpdateModel(Updates);
                    }
                    //for (int j = 0; j < Updates.size(); j++) {
                    //    Updates.set(j, Updates.get(j) + 1);
                    //}
                    Send_Ack(clientSocket);
                    clientSocket.close();
                } else if (task == 3) {
                    Return_Global_model(ipls_daemon.Get_Partitions(), clientSocket);
                    _TRAINING_START = (int) Instant.now().getEpochSecond();
                    //Return_Global_model(Updates,clientSocket);
                    System.out.println("Returned global model");
                    clientSocket.close();
                } else {
                    //Ipls.terminate
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
