import io.ipfs.api.IPFS;
import org.javatuples.Pair;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Bootstraper_Services extends Thread{
    public static int iter = 0;
    public static MyIPFSClass ipfsClass;
    public static IPFS ipfs;
    public static String Scheduler_filename = "Scheduler";
    public static int begin_time;
    public static int training_time;
    public static int Aggregation_time;
    public static int replicas_sync_time;
    public static String View_Hash;
    IPLS_DS DS;
    public Bootstraper_Services(String Path,int begin_time, int training_time, int Aggregation_time, int replicas_sync_time){
        ipfsClass = new MyIPFSClass(Path);
        ipfs = new IPFS(Path);
        this.begin_time = begin_time;
        this.training_time = training_time;
        this.Aggregation_time = Aggregation_time;
        this.replicas_sync_time = replicas_sync_time;
    }

    public Bootstraper_Services(String Path, String Scheduler_filename,int begin_time, int training_time, int Aggregation_time, int replicas_sync_time, String View_Hash){
        ipfsClass = new MyIPFSClass(Path);
        ipfs = new IPFS(Path);
        this.begin_time = begin_time;
        this.training_time = training_time;
        this.Aggregation_time = Aggregation_time;
        this.replicas_sync_time = replicas_sync_time;
        this.View_Hash = View_Hash;
    }

    // Schedule method is called by the bootstraper in order to create a training schedule. Firstly the developer has to enter the
    // begin time of the training, select the training time, aggregation phase time and replicas synchronization time. Upon training time
    // elapses, then the peers proceed to the aggregation time until this time elapses and then peers responsible for the same partition enter
    // replicas synchronization time, where they aggregate their partitions to form a global model.
    public List<Integer> Schedule(int begin_time, int training_time, int Aggregation_time, int replicas_synchronization_time, int epochs){
        List<Integer> schedule = new ArrayList<>();
        int time = 0;
        time += begin_time;
        schedule.add(time);
        for(int i =  0; i < epochs; i++){
            time += training_time;
            schedule.add(time);
            time += Aggregation_time;
            schedule.add(time);
            time += replicas_synchronization_time;
            schedule.add(time);
            schedule.add(iter);
        }
        return schedule;
    }

    // Publish the new Schedule into your network
    public void publish_schedule(int begin_time, int training_time, int Aggregation_time, int replicas_synchronization_time, int epochs) throws Exception{

        PeerData.current_schedule =  Schedule(begin_time, training_time, Aggregation_time, replicas_synchronization_time, epochs);
        String Schedule_Hash  = ipfsClass._Upload_File(
                new Pair<>(PeerData.current_schedule,View_Hash),
                Scheduler_filename
        ).toString();
        System.out.println(Schedule_Hash);
        PeerData.Schedule_Hash = Schedule_Hash;
        ipfs.pubsub.pub("New_Peer",ipfsClass.Marshall_Packet(Schedule_Hash,PeerData._ID,0,(short) 15));
    }

    public void storedirectory(byte[] id,List<byte[]> aggr, List<Integer> partitions,List<byte[]> hashes){
        DS.storeGradients(id,aggr,partitions,hashes);
    }

    public  void run(){
        int round = 0;
        DS = new IPLS_DS(PeerData.Path,false);
        DS.start();
        DS.set_round(round);
        iter = 0;
        while(true){
            try {
                publish_schedule((int) Instant.now().getEpochSecond() + begin_time,  training_time,Aggregation_time,replicas_sync_time,1);
                System.out.println("Published new schedule : " + PeerData.Schedule_Hash + " , " + ipfsClass.training_elapse_time(iter) + " , " + ipfsClass.get_curr_time());
                Thread.sleep((ipfsClass.training_elapse_time(iter) - ipfsClass.get_curr_time() - 3)*1000);
                System.out.println(">>>>>Clearing Updates ");
                DS.clear_updates_table();
                //Thread.sleep((begin_time + 2*(training_time+Aggregation_time+replicas_sync_time))*1000);
                System.out.println("Waiting : " + PeerData.current_schedule.get(PeerData.current_schedule.size()-2) + " , " + Instant.now().getEpochSecond());
                while(Instant.now().getEpochSecond()  < PeerData.current_schedule.get(PeerData.current_schedule.size()-2) &&
                        (PeerData.premature_termination == false || (PeerData.premature_termination == true && PeerData.flush == false))){Thread.yield();}
                System.out.println(PeerData.flush + " , " + PeerData.current_schedule.get(PeerData.current_schedule.size()-2) + " , " + Instant.now().getEpochSecond());
                iter++;
                DS.set_round(iter);
                DS.flush_ds(true,true);
                if(PeerData.premature_termination){PeerData.flush = false;}

                //System.out.println(Instant.now().getEpochSecond() + ", " + PeerData.current_schedule.get(PeerData.current_schedule.size()-2) );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
