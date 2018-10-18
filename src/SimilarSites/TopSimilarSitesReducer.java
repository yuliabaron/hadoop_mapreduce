package SimilarSites;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class TopSimilarSitesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> val = values.iterator();
        for (int i = 0; i < 10 && val.hasNext(); i++) {
            String outputKey = val.next().toString();
            outputKey = outputKey.replace(":", "\t");
            context.write(new Text(outputKey), new Text(""));
        }
    }
}