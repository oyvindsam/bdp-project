package au.edu.rmit.bdp.clustering.model;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class DpArrayWritable extends ArrayWritable {


    public DpArrayWritable() {
        super(DataPoint.class);
    }

    public DpArrayWritable(Writable[] values) {
        super(DataPoint.class, values);
    }
}
