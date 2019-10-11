package au.edu.rmit.bdp.clustering.model;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class CentArrayWritable extends ArrayWritable {


    public CentArrayWritable() {
        super(Centroid.class);
    }

    public CentArrayWritable(Writable[] values) {
        super(Centroid.class, values);
    }
}
