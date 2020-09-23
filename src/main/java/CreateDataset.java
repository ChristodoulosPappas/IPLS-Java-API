import org.deeplearning4j.datasets.fetchers.DataSetType;
import org.deeplearning4j.datasets.iterator.impl.Cifar10DataSetIterator;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;

public class CreateDataset {

    public static void main(String[] argc){
        int i = 0;
        DataSetIterator iter = new Cifar10DataSetIterator(1, DataSetType.TRAIN);
        for(i= 0; i < 100; i++){
            System.out.println(iter.next().getLabels());
        }
        System.out.println(i);
        System.out.println("OK");
    }

}
