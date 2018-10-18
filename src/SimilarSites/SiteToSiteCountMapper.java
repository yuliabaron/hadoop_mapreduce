package SimilarSites;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SiteToSiteCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        IntWritable one = new IntWritable(1);
        String val = value.toString();
        String[] sitesPair = val.split("\t");

        if (sitesPair.length != 2) {
            throw new IOException(String.format("Invalid input pair: %s", val));
        }

        //create site to site pair, each pair is created twice - in a straight forward and reversed order
        output.collect(new Text(sitesPair[0]+":"+sitesPair[1]), one);
        output.collect(new Text(sitesPair[1]+":"+sitesPair[0]), one);
    }
}
