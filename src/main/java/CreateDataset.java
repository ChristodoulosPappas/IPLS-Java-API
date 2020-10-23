import org.deeplearning4j.datasets.fetchers.DataSetType;
import org.deeplearning4j.datasets.iterator.impl.Cifar10DataSetIterator;
import org.deeplearning4j.datasets.iterator.impl.EmnistDataSetIterator;
import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator;
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.evaluation.classification.Evaluation;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Sgd;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.nd4j.shade.guava.primitives.Doubles;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

import static org.deeplearning4j.datasets.iterator.impl.EmnistDataSetIterator.Set.LETTERS;

public class CreateDataset {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        /*
        int i = 0;
        DataSetIterator iter = new Cifar10DataSetIterator(1, DataSetType.TRAIN);
        for(i= 0; i < 100; i++){
            System.out.println(iter.next().getLabels());
        }
        System.out.println(i);
        System.out.println("OK");

         */
        //String Path = args[0];
        List<String> Bootstrapers = new ArrayList<>();
        boolean isBootstraper;
        final int numRows = 28;
        final int numColumns = 28;
        int outputNum = 10; // number of output classes
        int batchSize = 60; // batch size for each epoch
        int rngSeed = 123; // random number seed for reproducibility
        int numEpochs = 15; // number of epochs to perform
        double rate = 0.0015; // learning rate
        int i;
        DataSetIterator lfw = null,mnistTrain = null,mnistTest = null;
        DataSet dataset = null;
        try {
            //Get the DataSetIterators:

            lfw = new EmnistDataSetIterator(LETTERS,batchSize,true);

            mnistTrain = new MnistDataSetIterator(batchSize, true, rngSeed);
            // System.out.println(mnistTrain.next(1).getLabels().toStringFull());
            //System.out.println(mnistTrain.next(1).getFeatures().toStringFull());
            mnistTest = new MnistDataSetIterator(batchSize, false, rngSeed);
        }
        catch (Exception e){
            System.out.println("Could not find iterator ");
        }

        //MNiST : 1000
        //lfw : 1480
        INDArray TotalInput = Nd4j.zeros(60000,784);
        INDArray TotalLabels = Nd4j.zeros(60000,10);
        INDArray batchIn = Nd4j.zeros(100,784);
        INDArray batchOut = Nd4j.zeros(100,10);

        INDArray Input;
        INDArray Output;
        INDArray Input2 = null;
        INDArray Output2 = null;
        int counter = 0;
        DataSet Data;


        for(int k = 0; k < 1000 && mnistTrain.hasNext(); k++){
            Data = mnistTrain.next();
            for(int j = 0; j < Data.getFeatures().rows(); j++){
                TotalInput.putRow(counter,Data.getFeatures().getRow(j));
                TotalLabels.putRow(counter,Data.getLabels().getRow(j));
                counter++;
            }
        }
        List<Integer> Partitions = new ArrayList<>();
        int[] arr = new int[10];
        int[] putIndex = new int[10];
        int index ;
        int datasize = 0;
        int nodes = new Integer(args[0]);
        int[] class_data = new int[10];
        int[] class_index = new int[10];
        for(i = 0; i <  60000; i++){
            arr[Doubles.asList(TotalLabels.getRow(i).toDoubleVector()).indexOf(1.0)]++;
        }
        Map<Integer, INDArray> DataMap = new HashMap<>();
        Map<Integer, INDArray> LabelsMap = new HashMap<>();
        for(i = 0 ; i < 10; i++){
            DataMap.put(i,Nd4j.zeros(arr[i],784));
            LabelsMap.put(i,Nd4j.zeros(arr[i],10));
            putIndex[i] = 0;
            System.out.println(arr[i]);
            class_data[i] = (int)arr[i]/nodes;
            System.out.println(class_data[i]);
            System.out.println("=========");

            class_index[i] = 0;
        }
        for(i = 0; i < 60000; i++){
            index = Doubles.asList(TotalLabels.getRow(i).toDoubleVector()).indexOf(1.0);
            DataMap.get(index).putRow(putIndex[index],TotalInput.getRow(i));
            LabelsMap.get(index).putRow(putIndex[index],TotalLabels.getRow(i));
            putIndex[index]++;
        }
        int node = 2;

        for(i = 0; i < nodes; i++){
            int dataset_size = 0;
            for(int j =0 ; j < 10; j++){
                dataset_size+= class_data[j];
            }
            System.out.println(dataset_size);
            Input = Nd4j.zeros(dataset_size,784);
            Output = Nd4j.zeros(dataset_size,10);
            index= 0;
            for(int j = 0; j < 10; j++){
                System.out.println("Getting from : " + class_index[j] + " to " +  new Integer(class_index[j] + class_data[j]) + " of " + j);

                for(int k = class_index[j]; k < class_index[j] + class_data[j] ; k++){
                    Input.putRow(index, DataMap.get(j).getRow(k));
                    Output.putRow(index, LabelsMap.get(j).getRow(k));
                    index++;
                }
                class_index[j] += class_data[j];
            }
            Random rand = new Random();

            for (int j = 0; j < dataset_size; j++) {
                int randomIndexToSwap = rand.nextInt(dataset_size);
                INDArray tempInput = Input.getRow(randomIndexToSwap);
                INDArray tempOutput = Output.getRow(randomIndexToSwap);
                Input.putRow(randomIndexToSwap,Input.getRow(j));
                Output.putRow(randomIndexToSwap,Output.getRow(j));
                Input.putRow(j,tempInput);
                Output.putRow(j,tempOutput);

            }
            DataSet myData = new DataSet(Input,Output);
            List<DataSet> Dlist = myData.asList();
            DataSetIterator mni = new ListDataSetIterator(Dlist,30);

            FileOutputStream fos = new FileOutputStream(node+"TrainDataset");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(mni);
            oos.close();
            fos.close();

            node++;
        }

        int test =0 ;
        for(i = 0; i < 10; i++){
            test+= arr[i];
        }
        System.out.println(test);

        /*
        for(int j = 2; j < 18; j++){
            System.out.println(j);
            int dataset_size = 3000;
            Random rn = new Random();
    	    int pos,index = 0;
    	    int n = 60000;
    	    List<Integer> DataPool = new ArrayList<>();
    	    for(i = 0; i < n; i++) {
    		    DataPool.add(i);
    	    }
    	    Input = Nd4j.zeros(dataset_size,784);
    	    Output = Nd4j.zeros(dataset_size,10);
           	for(i = 0; i < dataset_size ; i++ ) {
    		    pos = Math.abs(rn.nextInt()%n);
    		    Input.putRow(index, TotalInput.getRow(DataPool.get(pos)));
    	    	Output.putRow(index, TotalLabels.getRow(DataPool.get(pos)));
    	    	n--;
    		    index++;
    		    DataPool.remove(pos);
    	    }
            DataSet myData = new DataSet(Input,Output);
            List<DataSet> Dlist = myData.asList();
            DataSetIterator mni = new ListDataSetIterator(Dlist,100);

            FileOutputStream fos = new FileOutputStream(j+"TrainDataset");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(mni);
            oos.close();
            fos.close();
        }

         */
        /*
		#SAVE DATASETS
        DataSet myData = new DataSet(TotalInput,TotalLabels);
        List<DataSet> Dlist = myData.asList();
        DataSetIterator mni = new ListDataSetIterator(Dlist,100);

        FileOutputStream fos = new FileOutputStream("MnistTest");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(mni);
        oos.close();
        fos.close();
		*/
        
        
		/*
        // INITIALIZE ETH FILE
        List<Double> L = new ArrayList<>();
        FileOutputStream fos = new FileOutputStream("ETHModel");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        System.out.println(model.params().length());
        oos.writeObject(Doubles.asList(model.params().getRow(0).toDoubleVector()));
        oos.close();
        fos.close();
        //System.out.println(mnistTrain.next(10).);
         */

        /*
		// CHECKING SERIALIZATION
        IPFS ipfs = new IPFS(Path);
        MyIPFSClass ipfsClass = new MyIPFSClass(Path);
        System.out.println((short) ipfs.id().get("ID").toString().length());
        String decodedString = ipfsClass.Marshall_Packet(ipfs.id().get("ID").toString(),ipfs.id().get("ID").toString(),2,(short)3);
        byte[] bytes_array = Base64.getUrlDecoder().decode(decodedString);
        int OriginPeerSize,PeerSize;
        List<Integer> Peer_Auth = new ArrayList<Integer>();
        ByteBuffer rbuff = ByteBuffer.wrap(bytes_array);
        String Renting_Peer = null,Origin_Peer= null;
        //Get Pid
        rbuff.getShort();
        System.out.println(Get_ACK(rbuff,bytes_array));


         */


        /*

        /// SIMPLE DEBUGING PROGRAM

        First_Application IPLS = new First_Application();
        IPLS.init(Path);

        List<Double> Param = new ArrayList<>();
        List<Double> Gradients = new ArrayList<>();

        for(i = 0; i < 100; i ++){
            Gradients.add(-1.0);
        }

        for(i = 0; i < 15; i++){
            Param = IPLS.GetPartitions();
            System.out.println(Param);
            Thread.sleep(2000);
            IPLS.UpdateGradient(Gradients);
            Thread.sleep(4000);
        }

         */


        /*


        if(args[i].equals("r")){
            int begin = new Integer(args[i+1]);
            int end = new Integer(args[i+2]);
            Input = Nd4j.zeros(end - begin,784);
            Output = Nd4j.zeros(end - begin,10);
            for(i = begin; i < end; i++){
                Output.putRow(i - begin, TotalLabels.getRow(i));
                Input.putRow(i - begin, TotalInput.getRow(i));
            }

        }
        else{
            List<Integer> Partitions = new ArrayList<>();
            int[] arr = new int[10];
            int[] putIndex = new int[10];
            int index;
            int datasize = 0;
            for(int j = i +1; j < args.length-1; j++){
                Partitions.add(new Integer(args[j]));
            }
            for(i = 0; i <  10000; i++){
                arr[Doubles.asList(TotalLabels.getRow(i).toDoubleVector()).indexOf(1.0)]++;
            }
            Map<Integer, INDArray> DataMap = new HashMap<>();
            Map<Integer, INDArray> LabelsMap = new HashMap<>();
            for(i = 0 ; i < 10; i++){
                DataMap.put(i,Nd4j.zeros(arr[i],784));
                LabelsMap.put(i,Nd4j.zeros(arr[i],10));
                putIndex[i] = 0;
                System.out.println(arr[i]);
            }
            for(i = 0; i < 10000; i++){
                index = Doubles.asList(TotalLabels.getRow(i).toDoubleVector()).indexOf(1.0);
                DataMap.get(index).putRow(putIndex[index],TotalInput.getRow(i));
                LabelsMap.get(index).putRow(putIndex[index],TotalLabels.getRow(i));
                putIndex[index]++;

            }

            for( i = 0; i < Partitions.size(); i++){
                datasize += arr[Partitions.get(i)];
            }
            System.out.println(datasize);

            Input = Nd4j.zeros(datasize,784);
            Output = Nd4j.zeros(datasize,10);
            Input2 = Nd4j.zeros(    10000-datasize,784);
            Output2 = Nd4j.zeros(10000-datasize,10);

            index = 0;
            for(i = 0; i < Partitions.size(); i++){
                for(int j = 0; j < DataMap.get(Partitions.get(i)).rows(); j++){
                    Input.putRow(index,DataMap.get(Partitions.get(i)).getRow(j));
                    Output.putRow(index,LabelsMap.get(Partitions.get(i)).getRow(j));
                    index++;
                }
            }
            index = 0;
            List<Integer> P2 = new ArrayList<>();
            for(i = 5; i < 10; i++){
                P2.add(i);
            }
            for(i = 0; i < P2.size(); i++){
                for(int j = 0; j < DataMap.get(P2.get(i)).rows(); j++){
                    Input2.putRow(index,DataMap.get(P2.get(i)).getRow(j));
                    Output2.putRow(index,LabelsMap.get(P2.get(i)).getRow(j));
                    index++;
                }
            }
        }

        INDArray Dumm = Nd4j.zeros(1,443610);
        INDArray gradient2 = Nd4j.zeros(1,443610);
        INDArray gradient = Nd4j.zeros(1,443610);
        Random rand = new Random();



        int randomNum ;
        List<Double> arr = new ArrayList<>();
        List<Double> acc = new ArrayList<>();
        DataSet myData = new DataSet(Input,Output);
        List<DataSet> Dlist = myData.asList();
        DataSetIterator mni = new ListDataSetIterator(Dlist,100);
        for(i = 0; i < 100; i++){
            //model.fit(Input,Output);
            for(int j = 0; j < 443610;j++){
                Dumm.put(0,j,model.params().getDouble(j));
            }
            System.out.println(Dumm);
            System.out.println(model.params());
            model.fit(mni,1);
            System.out.println("===========");
            System.out.println(model.params());
            for(int j = 0; j < Dumm.length(); j++){
                Dumm.put(0,j,Dumm.getDouble(j) - model.getGradientsViewArray().getRow(0).getDouble(j));
            }
            System.out.println(model.getGradientsViewArray().getRow(0));
            System.out.println(Dumm);
            model.setParams(Dumm);
            System.out.println("ITER : " + i);
            System.out.println("Evaluate model....");

            Evaluation eval = model.evaluate(mnistTest);
            System.out.println(eval.stats());
            System.out.println(eval.accuracy());
            acc.add(eval.accuracy());
            System.out.println("****************Example finished********************");

        }
        System.out.println("Evaluate model....");

        Evaluation eval = model.evaluate(mnistTest);
        System.out.println(eval.stats());
        System.out.println(eval.accuracy());
        acc.add(eval.accuracy());
        System.out.println("****************Example finished********************");
        */
    }

}
