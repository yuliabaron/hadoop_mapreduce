package SimilarSites;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopSimilarSitesGroupComparator extends WritableComparator {

    protected TopSimilarSitesGroupComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        //group by first site in the pair
        Text sitePair1 = (Text) w1;
        Text sitePair2 = (Text) w2;
        String[] sitePair1Items = sitePair1.toString().split(":");
        String[] sitePair2Items = sitePair2.toString().split(":");

        return sitePair1Items[0].compareTo(sitePair2Items[0]);
    }
}
