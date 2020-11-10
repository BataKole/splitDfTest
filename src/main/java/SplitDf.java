import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;

public class SplitDf {

    Session session = new Session();

    // List used to store Datasets, each containing 1000 records
    private ArrayList<Dataset<Row>> dfList = new ArrayList<Dataset<Row>>();

    // first load whole Json file to totalDf. SparkSession will be used for file loading
    private Dataset<Row> totalDf ;

    // this method takes totalDf and splits it into equal Datasets and internally adds //Datasets to ListArray<Dataset<Row> >
    private void splitDf(Dataset<Row> totalDf) {

        int i=0;
        Dataset<Row> temp;

        long recordCount = totalDf.count();
        System.out.println(recordCount);
        long residualRecords = 0;

        while(true) {

            String A = "gfcid >= " + String.valueOf(i * 1000);
            String C = "gfcid <= " +  String.valueOf((i + 1) * 1000);

            System.out.println(i);

            // If there is less than 1000 records left, take the rest of records   (residualRecords)
            if((i+1) * 1000 > recordCount) {

                residualRecords = recordCount - (i * 1000);
                String B = "gfcid <= " + String.valueOf((i * 1000)+ residualRecords);
                System.out.println(A + " and " + B);
                temp = totalDf.select(totalDf.col("*")).filter(A).filter(B);
            } else {
                // Take next 1000 records
                temp = totalDf.select(totalDf.col("*")).filter(A).filter(C);
            }


            // Append temp DF to dfList
            dfList.add(temp);

            // The loop went through whole totalDf
            if((i+1) * 1000 > recordCount) break;
            // Increase the counter
            i++;

        }
    }

    public ArrayList<Dataset<Row> > getDfList(String path){
        totalDf = session.loadDf(path);

        splitDf(totalDf);


        return dfList;
    }
}
