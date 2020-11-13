import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;

public class SplitDf {

    Session session = new Session();

    // List used to store Datasets, each containing totalRecords / partsNum records
    private ArrayList<Dataset<Row>> dfList = new ArrayList<Dataset<Row>>();


    // first load whole Json file to totalDf. SparkSession will be used for file loading
    private Dataset<Row> totalDf ;

    // this method takes totalDf and splits it into equal Datasets and internally adds //Datasets to ListArray<Dataset<Row> >
    private void splitDf(Dataset<Row> totalDf, int partsNum) {

        int i=0;
        Dataset<Row> temp;

        long recordCount = totalDf.count();

        long partSize;
        if(recordCount % partsNum == 0)
            partSize = recordCount / partsNum;
        else partSize = recordCount / partsNum +  1;


        //long residualRecords;

        while(true) {

            String A = "gfcid >= " + String.valueOf(i * partSize);
            String B = "gfcid <= " +  String.valueOf((i + 1) * partSize);

            temp = totalDf.select(totalDf.col("*")).filter(A).filter(B);

            // Append temp DF to dfList
            dfList.add(temp);

            // The loop went through whole totalDf
            if((i+1) * partSize > recordCount) break;
            // Increase the counter
            i++;

        }
    }

    public ArrayList<Dataset<Row> > getDfList(String path,int partsNum){
        totalDf = session.loadDf(path);

        // Divide totalDf to partsNum DFs
        splitDf(totalDf, partsNum);

        return dfList;
    }
}
