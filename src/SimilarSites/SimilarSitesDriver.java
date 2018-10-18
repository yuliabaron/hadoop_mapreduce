package SimilarSites;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

public class SimilarSitesDriver {

    public static void fsCleanup (Path path) {
        Configuration conf = new Configuration();

        try {
            FileSystem hdfs = FileSystem.get(conf);

            if (hdfs.exists(path)) {
                hdfs.delete(path, true);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean validateInputPath(Path path) {
        Configuration conf = new Configuration();

        try {
            FileSystem hdfs = FileSystem.get(conf);

            if (!hdfs.exists(path)) {
                return false;
            }
        }catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Invalid parameters. Was expecting for input and output paths only");
            return;
        }

        if (validateInputPath(new Path(args[0])) != true) {
            System.out.println("Invalid input path");
            return;
        }

        //remove folders from the previous run
        fsCleanup(new Path(args[1]+"_intermediate0"));
        fsCleanup(new Path(args[1]+"_intermediate1"));
        fsCleanup(new Path(args[1]));

        try {
            if (TagToSiteDriver.run(args[0], args[1] + "_intermediate0") != true) {
                System.out.println("Failed to run TagToSiteDriver");
                return;
            }

            if (SiteToSiteCountDriver.run(args[1] + "_intermediate0", args[1] + "_intermediate1") != true) {
                System.out.println("Failed to run TagToSiteDriver");
                return;
            }

            if (TopSimilarSitesDriver.run(args[1] + "_intermediate1", args[1]) != true) {
                System.out.println("Failed to run TagToSiteDriver");
                return;
            }
        }catch (Exception e) {
            e.printStackTrace();
        }

        //clean the intermediate files
        fsCleanup(new Path(args[1]+"_intermediate0"));
        fsCleanup(new Path(args[1]+"_intermediate1"));
    }
}
