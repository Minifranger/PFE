package localKmeans;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by quentin on 05/03/17.
 */
public class Main {



    public static void main(String[] args) {


        KMeans km = new KMeans();


        List<Point> l = new ArrayList<Point>();
        l.add(new Point(1));
        l.add(new Point(2));
        l.add(new Point(1.5));
        l.add(new Point(10));
        l.add(new Point(11));

        km.setPoints(l);

        km.setNUM_POINTS(l.size());

        Cluster c1 = new Cluster(1);
        c1.setCentroid(new Point(1));

        Cluster c2 = new Cluster(2);
        c2.setCentroid(new Point(2));

        List<Cluster> lc = new ArrayList<Cluster>();
        lc.add(c1);
        lc.add(c2);

        km.setClusters(lc);
        km.setNUM_CLUSTERS(lc.size());
        km.plotClusters();

        km.calculate();
    }
}
