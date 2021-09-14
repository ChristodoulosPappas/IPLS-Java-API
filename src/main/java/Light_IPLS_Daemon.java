import io.ipfs.api.IPFS;
import org.nd4j.autodiff.listeners.Loss;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class Light_IPLS_Daemon extends Thread{
    IPLS ipls;
    List<Double> Gradients = new ArrayList<>();
    List<Double> Update = new ArrayList<>();
    public static int got_partitions = 1;
    public static Semaphore wait_sem = new Semaphore(0);
    public static Semaphore Update_wait = new Semaphore(0);
    public static Semaphore mtx = new Semaphore(1);

    public Light_IPLS_Daemon(){
        this.ipls = null;
    }
    public Light_IPLS_Daemon(IPLS ipls){
        this.ipls = ipls;
    }

    public List<Double> Get_Partitions() throws Exception{
        wait_sem.acquire();
        return Update;
    }

    public void UpdateModel(List<Double> Gradients) throws Exception{
        mtx.acquire();
        PeerData.training_finished = true;
        this.Gradients = Gradients;
        mtx.release();

        Update_wait.acquire();


    }

    public boolean finised() throws Exception{
        mtx.acquire();
        boolean finised = PeerData.training_finished;
        mtx.release();
        return finised;
    }

    public void init(){
        try {
            ipls.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        MyIPFSClass Auxiliary = new MyIPFSClass();

        try {
            ipls.init();
        } catch (Exception e) {
            e.printStackTrace();
        }

        while(true){
            try {
                if(ipls != null){
                    Update = ipls.GetPartitions();
                }

                if(got_partitions == 1){
                    wait_sem.release();
                    got_partitions--;
                }
                if (PeerData.Relaxed_SGD) {
                    System.out.println("Time now : " + (int) Instant.now().getEpochSecond() +" , finishing time "  + Auxiliary.training_elapse_time(PeerData.middleware_iteration));
                    while (!PeerData.training_finished && (int) Instant.now().getEpochSecond() < Auxiliary.training_elapse_time(PeerData.middleware_iteration)) {
                        Thread.yield();
                    }
                    if (finised()) {
                        System.out.println("NODE SENDS GRADIENTS");
                        if(ipls != null){
                            ipls.UpdateGradient(Gradients);
                        }
                        PeerData.training_finished = false;
                        got_partitions++;
                        Update_wait.release();
                    }
                    else {
                        System.out.println("NODE DIDN'T TRAINED THE MODEL IN TIME");
                        if(ipls != null){
                            ipls.UpdateGradient(null);
                        }
                    }
                }
                else{
                    while (!PeerData.training_finished ) {
                        Thread.yield();
                    }
                    ipls.UpdateGradient(Gradients);
                    PeerData.training_finished = false;
                    got_partitions++;
                    Update_wait.release();
                }
            }
            catch (Exception e){
                System.out.println("Exception on IPLS daemon" + e);
            }
        }
    }


}
