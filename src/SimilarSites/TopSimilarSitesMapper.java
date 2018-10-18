package SimilarSites;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class TopSimilarSitesMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        String[] sitesCount = val.split("\t");
        Text newKey = new Text(sitesCount[0]+":"+sitesCount[1]);

        if (sitesCount.length != 2) {
            throw new IOException(String.format("Invalid input pair: %s", val));
        }

        //here we store key in value so we'll be able to access it in Reducer
        context.write(newKey, newKey);
    }
}
