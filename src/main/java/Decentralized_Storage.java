import java.util.concurrent.Semaphore;

public class Decentralized_Storage {


    public static void main(String[] argc) throws Exception{
        String path = argc[0];
        Semaphore InitSem = new Semaphore(0);

        Decentralized_Storage_Receiver receiver = new Decentralized_Storage_Receiver(path,InitSem);
        Decentralized_Storage_Discovery discovery_thread = new Decentralized_Storage_Discovery(path,InitSem);
        discovery_thread.start();
        receiver.start();
        InitSem.acquire();
        InitSem.acquire();
        Thread.sleep(1000);
        System.out.println("Storage Node initialized!");

    }
}
