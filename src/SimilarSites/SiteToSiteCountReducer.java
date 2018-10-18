package SimilarSites;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SiteToSiteCountReducer extends MapReduceBase implements Reducer <Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce (Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        /* Here we get the list of all the sites sharing the same tag.
           Go over the list output comma separated list of sites sharing same tag (per tag)
         */
        int count = 0;
        while (values.hasNext()) {
            count += values.next().get();
        }

        output.collect(key, new IntWritable(count));
    }
}
