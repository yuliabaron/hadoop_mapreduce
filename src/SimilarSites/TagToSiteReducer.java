package SimilarSites;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TagToSiteReducer extends MapReduceBase implements Reducer <Text, Text, Text, Text> {

    @Override
    public void reduce (Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        /* Here we get the list of all the sites sharing the same tag.
           Go over the list and create pairs of similar sites
         */
        List<String> sitesList = new ArrayList<String>();
        while (values.hasNext()) {
            sitesList.add(values.next().toString());
        }

         for (int i = 0; i < sitesList.size(); i++) {
            for (int j = i+1; j < sitesList.size(); j++) {
                 output.collect(new Text(sitesList.get(i)), new Text(sitesList.get(j)));
             }
        }
    }
}
