package SimilarSites;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopSimilarSitesPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        //partition by first site in the pair
        String[] keyItems = key.toString().split(":");
        return keyItems[0].hashCode() % numPartitions;
    }
}