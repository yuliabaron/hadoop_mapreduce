package SimilarSites;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopSimilarSitesKeyCompataror extends WritableComparator {
    protected TopSimilarSitesKeyCompataror() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text sitePair1 = (Text) w1;
        Text sitePair2 = (Text) w2;
        String[] sitePair1Items = sitePair1.toString().split(":");
        String[] sitePair2Items = sitePair2.toString().split(":");

        //descending similarity count
        int comp = sitePair1Items[0].compareTo(sitePair2Items[0]);
        if (comp == 0) {
            comp = sitePair2Items[2].compareTo(sitePair1Items[2]);

            //if both pairs have the same similarity index - order lexicographically
            if (comp == 0) {
                comp = sitePair1Items[1].compareTo(sitePair2Items[1]);
            }
        }

        return comp;
    }
}