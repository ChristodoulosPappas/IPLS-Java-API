//import io.ipfs.api.Pair;
import io.ipfs.api.Peer;
import io.ipfs.multihash.Multihash;
import org.javatuples.*;
import org.nd4j.linalg.api.ops.custom.Tri;
import org.web3j.abi.datatypes.Int;

import javax.crypto.SecretKey;
import javax.naming.InsufficientResourcesException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class PeerData {

    public static Download_Scheduler aggregation_download_scheduler;
    public static Download_Scheduler partial_updates_download_scheduler;
    public static Download_Scheduler updates_download_scheduler;
    public static IPLS_DS_Client ds_client;
    public static DStorage_Client storage_client;
    public static DS_query_manager gradients_query_manager;
    public static DS_query_manager partial_updates_query_manager;

    public static Semaphore InitSem = new Semaphore(0);
    public static Semaphore mtx = new Semaphore(1);
    public static Semaphore com_mtx = new Semaphore(1);
    public static Semaphore SendMtx = new Semaphore(1);
    public static Semaphore weightsMtx = new Semaphore(1);
    public static Semaphore DS_mtx = new Semaphore(1);

    public static Map<Integer,List<String>> workers = new HashMap<>();
    public static Map<Integer,List<String>> Replica_workers = new HashMap<>();

    public static Map<Integer, Double> previous_iter_active_workers = new HashMap<>();

    // The number of partitions that we partition the model
    public static int _PARTITIONS;
    //The minimum number of responsibilities a peer must have
    public static int _MIN_PARTITIONS;
    // Set the training time of the learning process
    public static int Training_time;
    // The number of parameters of the model
    public static long _MODEL_SIZE;
    // Boolean value inficating if the system is on the initialization phase. Becomes false when it proceed on training phase
    public static boolean First_Iter = true;
    // Boolean value indicating if the system is using SGD or Asynchronous GD
    public static boolean isSynchronous = true;
    public static boolean Relaxed_SGD = true;
    // This variable indicates if the system enables premature terminatio
    // Premature termination is whenever all partitions updated before synchronization
    // elapse time so the IPLS peers don't have to wait until the elapse of synchronization
    // phase, but proceed immidiatelly when all update hashes received
    public static boolean premature_termination = true;
    // This variable indicates whenever an aggregator on premature termination didn't downloaded
    // all partial updates and his wait_ack list erased
    public static boolean flush = false;
    public static boolean isBootsraper;
    public static boolean training_finished = false;
    public static int _Iter_Clock = 0;
    public static String _ID = null;
    public static String Schedule_Hash = null;
    public static String MyPublic_Multiaddr = null;
    public static int Index = 0;
    public static int used_commitments = 0;
    public static int is_alive_counter = 0;
    // The global logical clock of the system indicating the global iteration. This is used only in SGD
    public static int middleware_iteration = 0;
    public static List<String> Members = new ArrayList<>();
    public static int Min_Members;
    public static boolean training_phase = false;
    public static boolean IPNS_Enable = false;
    public static boolean _is_test = false;
    // This variable, checks if partial aggregation feature
    // is true. If this happens, then if an aggregator find
    // an IPFS peer that stores more than one gradient partitions
    // for the partition he is responsible for, then he can send
    // a merge request so that those partitions are going to
    // be partially aggregated and sent to the server
    public static boolean Partial_Aggregation = false;
    public static boolean local_save = false;
    // In case indirect communication is true then IPLS peers
    // upload their data to the decentralized web instead of
    // communicating directly with other IPLS peers.
    public static boolean Indirect_Communication = false;
    volatile public static int STATE = 0;


    public static List<Integer> current_schedule = new ArrayList<>();

    //These variables are used for analyzing the system
    public static Semaphore Test_mtx = new Semaphore(1);
    public static List<Integer> RecvList = new ArrayList<>();
    // This list indicates which partitions where actually stored
    // in the current IPLS round
    //public static List<Integer> Partitions_committed = new ArrayList<>();
    public static Map<Integer,List<Triplet<Integer,Integer,String>>> Partitions_committed = new HashMap();
    public static int DataRecv = 0;

    //Blocking Queue, for any task given to the updater
    public static BlockingQueue<Sextet<List<String>,Integer,Integer, Boolean ,double[],String>> queue = new LinkedBlockingQueue<Sextet<List<String>,Integer,Integer, Boolean ,double[],String>>();
    //Blocking Queue, for Global Gradients Pool
    public static BlockingQueue<String> GGP_queue = new LinkedBlockingQueue<>();
    //Blocking Queue, for Global Gradients Authority updates
    public static BlockingQueue<Pair<Integer,Integer>> UpdateQueue = new LinkedBlockingQueue<>();
    //The list of the ipfs nodes available in the system
    public static BlockingQueue<String> ipfsNodesQueue = new LinkedBlockingQueue<>();
    //This data structure is used so that the peers can get the data of the
    // partitions that they become responsible for
    public static Map<Integer,String> Hash_Partitions = new HashMap<>();
    public static Map<String,String> Downloaded_Hashes = new HashMap<>();
    public static Map<String, SecretKey> Hash_Keys = new HashMap<>();
    public static Map<Integer,List<Triplet<String,String,Integer>>> Committed_Hashes = new HashMap<>();
    public static Map<Integer,Pair<String,SecretKey>> key_dir = new HashMap<>();



    public static Map<Integer,List<Double>> GradientPartitions = new HashMap<>();
    //public static List<Double> Gradients = new ArrayList<Double>();
    //The gradients received from replica peers
    public static Map<Integer,double[]> Replicas_Gradients = new HashMap<>();
    // This data structure is used in order to store the extra gradients an aggregator downloaded
    // in the aggregation phase
    public static Map<Pair<Integer,String>,double[]> Other_Replica_Gradients = new HashMap<>();
    public static Map<Pair<Integer,String>,Integer> Other_Replica_Gradients_Received = new HashMap<>();
    public static List<Pair<Integer,String>> Received_Replicas = new ArrayList<>();
    // The gradients received from my clients (peers that are not responsible for my partitions so i am a dealer of them)
    public static Map<Integer,double[]> Aggregated_Gradients = new HashMap<>();
    // There is a very very small and almost impossible for realsitic systems chance that a client has finished the next iteration while i am in the previous (just before end the updateGradientMethod)
    // This Structure aggregate those gradients from fututre
    public static Map<Integer,List<Double>> Aggregated_Gradients_from_future = new HashMap<>();
    public static Map<Integer,double[]> Stored_Gradients = new HashMap<>();
    public static Map<Integer,double[]> Weights = new HashMap<Integer, double[]>();

    public static boolean sendingGradients = false;
    //List that shows for the peers that have not yet replied
    public static List<Triplet<String,Integer,Integer>> Wait_Ack = new ArrayList<Triplet<String,Integer,Integer>>();
    public static List<Triplet<String,Integer,Integer>> Wait_Ack_from_future = new ArrayList<Triplet<String,Integer,Integer>>();

    //List that shows the clients that have not yet sent gradients. I must wait until Client_Wait_Ack size is 0
    public static List<Triplet<String,Integer,Integer>> Client_Wait_Ack = new ArrayList<>();
    // The same somehow with the Aggregated_Gradients_from_future
    public static List<Triplet<String,Integer,Integer>> Client_Wait_Ack_from_future = new ArrayList<>();
    // The same as Client_Wait_Ack but for the replicas synchronization
    public static List<Triplet<String,Integer,Integer>> Replica_Wait_Ack = new ArrayList<>();
    public static List<Triplet<String,Integer,Integer>> Replica_Wait_Ack_from_future = new ArrayList<>();

    //Hash that give us the latest update of each partition
    public static Map<Integer, Integer> Participants = new HashMap<>();


    public static String  Path;
    // In case Indirect_Communication is true then
    // the provider is the storage node on whom an
    // IPLS peer provides his data
    public static String Provider;
    //List of partitions that a peer is responsible for
    public static List<Integer> Auth_List = new ArrayList<Integer>();
    //List of string with the unique ids of peers
    public static List<String> Existing_peers = new ArrayList<String>();
    //Peers that left must also be removed from data structures
    public static List<String> Leaving_peers = new ArrayList<String>();
    public static List<String> Bootstrapers = new ArrayList<>();
    public static List<String> updates_hashes = new ArrayList<>();
    public static List<Peer> peers = new ArrayList<Peer>();
    //Hash table in the form : [Authority_id,[Selected_Peers]]
    public static Map<Integer,List<String>> Partition_Availability = new HashMap<Integer,List<String>>();
    // Hash table showing the peers that the daemo communicates
    public static Map<Integer,String> Dealers = new HashMap<Integer, String>();
    //Hash table in the form : [Swarm_Peer,[His Authority]]
    public static Map<String,List<Integer>> Swarm_Peer_Auth = new HashMap<String,List<Integer>>();
    //Hash table that contains that contains the hash value of the file, with key based on partition
    public static Map<Integer,double[]> Weight_Address = new HashMap<Integer, double[]>();
    //Hash table that contains the clients of a peer, aka those who
    // send to him the gradients. This data structure is been used
    // only in Synchronous SGD
    public static Map<Integer,List<String>> Clients = new HashMap<>();
    //This is a hash table that keeps into account the new clients joined in the
    // system in order to make them officially acceptable after the iteration
    // is finished
    public static Map<Integer,List<String>> New_Clients = new HashMap<>();
    public static Map<Integer,List<String>> New_Replicas = new HashMap<>();
    public static Map<Integer,List<String>> New_Members = new HashMap<>();
    public static Map<Integer,List<String>> Replica_holders = new HashMap<>();
    public static Map<String,Integer> Servers_Iteration = new HashMap<>();
    public static Map<String,Integer> Clients_Iteration = new HashMap<>();


    //For testings
    public static int num;
    // This structure maintains a log that contains important debugging and other data
    public static Map<String,List<Object>> _LOG = new HashMap<>();
    public static Map<String,Integer> _PEER_DOWNLOAD_TIME = new HashMap<>();
    public static int commited_hashes = 0;
    public static int downloaded_hashes = 0;
    public static int downloaded_updates = 0;
    public static DebugInfo dlog;
}
