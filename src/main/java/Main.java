import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class Main {




    public static void main(String[] args) {


        SplitDf splitDf = new SplitDf();

        String jsonPath = "./data/jsonTest.json";

        ArrayList<Dataset<Row>> dfList = splitDf.getDfList(jsonPath);

        System.out.println("List lenght: " + dfList.size());

        for (Dataset<Row> df: dfList) {
            df.show();
        }

    }
}
