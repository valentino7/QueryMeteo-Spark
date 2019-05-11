package Utils;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple2<String, Double>>, Serializable {

    @Override
    public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) { // -1 minore, +1 maggiore, 0 uguale
        return o2._2().compareTo(o1._2());
    }
}
