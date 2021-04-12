import io.ipfs.api.IPFS;
import io.ipfs.api.KeyInfo;
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.learning.config.Sgd;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.nd4j.shade.guava.primitives.Doubles;

import org.apache.commons.cli.*;
import org.web3j.abi.datatypes.Int;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class MnistTest{
        public static void alternative() throws IOException, ClassNotFoundException {
                MyIPFSClass ipfs = new MyIPFSClass("/ip4/127.0.0.1/tcp/5004");
                System.out.println(ipfs.check_peer("12D3KooWRrcW2QLp6dWXnrCGyoQFehLYEpqzaaEi798qhPvjePbj") );
                ipfs.get_file(ipfs.check_peer("12D3KooWRrcW2QLp6dWXnrCGyoQFehLYEpqzaaEi798qhPvjePbj") + "/Auxiliaries");
        }

        public static void upload() throws Exception{
                Map<Integer,List<Double>> gradients = new HashMap<>();
                PeerData._PARTITIONS = 10;
                MyIPFSClass ipfs = new MyIPFSClass("/ip4/127.0.0.1/tcp/5005");
                ipfs.initialize_IPLS_directory();
                for(int i = 0; i < 100; i++){

                        for(int j = 0; j < 10; j++){
                                gradients.put(j,new ArrayList<>());
                                for(int k = 0; k < 5000; k++){
                                        gradients.get(j).add((double)k*i);
                                }
                        }
                        ipfs.publish_gradients(gradients,2);
                        System.out.println("OK");
                        Thread.sleep(5000);
                        PeerData.middleware_iteration++;
                }
        }
        public static void parse_args(){


        }

        public static void main(String[] args) throws Exception{
                int new_key,pid;
                Map<Integer,List<Double>> arr = new HashMap<>();
                MyIPFSClass ipfs = new MyIPFSClass("/ip4/127.0.0.1/tcp/5001");
                Random rd = new Random();
                PeerData._ID = "12D3KooWRrcW2QLp6dWXnrCGyoQFehLYEpqzaaEi798qhPvjePbj";
                Map<Integer,Integer> ser = new HashMap<>();
                System.out.println(ser.get(1) == 2);
                //ipfs.initialize_IPLS_directory();
                //ipfs.publish_gradients(arr,2);
                //ipfs.DownloadParameters("/ipfs/QmPJzwi5ppuh6jKX1bJ1KMotAjzeHS66p9KjxiqRzmAZ2p/7_Updates");

                /*
                PeerData._ID = "CXYZ";
                String decodedString  = ipfs._START_TRAINING();
                byte[] bytes_array = Base64.getUrlDecoder().decode(decodedString);
                int OriginPeerSize,PeerSize;
                List<Integer> Peer_Auth = new ArrayList<Integer>();
                ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);
                String Renting_Peer = null,Origin_Peer= null;
                //Get Pid
                pid = rbuff.getShort();
                System.out.println(pid);
                System.out.println(ipfs.Get_Peer(rbuff,bytes_array,Short.BYTES).length());
                */
        }


}