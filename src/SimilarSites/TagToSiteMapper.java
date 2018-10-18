package SimilarSites;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TagToSiteMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String val = value.toString();
        String[] siteAndTag = val.split("\t");

        //check input validity - we got pair exactly
        if (siteAndTag.length != 2) {
            throw new IOException(String.format("Invalid input pair: %s", val));
        }

        //create tag to site mapping
        output.collect(new Text(siteAndTag[1]), new Text(siteAndTag[0]));
    }
}
