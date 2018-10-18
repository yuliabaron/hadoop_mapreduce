package SimilarSites;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class SiteToSiteCountDriver {

    public static boolean run(String input, String output) {
        boolean success = false;
        JobClient siteToSiteCountClient = new JobClient();
        JobConf siteToSiteCountConf = new JobConf(SiteToSiteCountDriver.class);
        siteToSiteCountConf.setJobName("SimilarSitesPairsCount");
        siteToSiteCountConf.setOutputKeyClass(Text.class);
        siteToSiteCountConf.setOutputValueClass(IntWritable.class);
        siteToSiteCountConf.setMapperClass(SiteToSiteCountMapper.class);
        siteToSiteCountConf.setReducerClass(SiteToSiteCountReducer.class);

        siteToSiteCountConf.setInputFormat(TextInputFormat.class);
        siteToSiteCountConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(siteToSiteCountConf, new Path(input));
        FileOutputFormat.setOutputPath(siteToSiteCountConf, new Path(output));

        siteToSiteCountClient.setConf(siteToSiteCountConf);

        try {
            final RunningJob runStatus = JobClient.runJob(siteToSiteCountConf);
            success = runStatus.isSuccessful();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return success;
    }
}

