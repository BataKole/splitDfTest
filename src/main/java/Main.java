import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class Main {

    public static void main(String[] args) {

        // First, ensure there are 2 args
        if (args.length != 2) {
            throw new IllegalArgumentException("Exactly 2 parameters required !");
        }
        String jsonPath = args[0];
        int partsNum = Integer.parseInt(args[1]);


        SplitDf splitDf = new SplitDf();


        ArrayList<Dataset<Row>> dfList = splitDf.getDfList(jsonPath, partsNum);

        System.out.println("List length: " + dfList.size());

        int i = 1;
        for (Dataset<Row> df: dfList) {
            df.show();

            // Save parts to output dir as json
            df.write().json("./data/output/out" + i + ".json");
            i++;
        }

    }
}
