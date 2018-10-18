package SimilarSites;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TagToSiteDriver {

    public static boolean run(String input, String output) {
        boolean success = false;
        JobClient tagToSiteClient = new JobClient();
        JobConf tagToSiteConf = new JobConf(TagToSiteDriver.class);
        tagToSiteConf.setJobName("SitePairsByTags");
        tagToSiteConf.setOutputKeyClass(Text.class);
        tagToSiteConf.setOutputValueClass(Text.class);
        tagToSiteConf.setMapperClass(TagToSiteMapper.class);
        tagToSiteConf.setReducerClass(TagToSiteReducer.class);

        tagToSiteConf.setInputFormat(TextInputFormat.class);
        tagToSiteConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(tagToSiteConf, new Path(input));
        FileOutputFormat.setOutputPath(tagToSiteConf, new Path(output));

        tagToSiteClient.setConf(tagToSiteConf);

        try {
            final RunningJob runStatus = JobClient.runJob(tagToSiteConf);
            success = runStatus.isSuccessful();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return success;
    }
}
