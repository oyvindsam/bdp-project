package au.edu.rmit.bdp.clustering.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import de.jungblut.math.DoubleVector;

/**
 * The class models the center of a cluster.
 * By implementng WritableComparable<Centroid> or Writable interface,
 * we can make our own key or value type that work with hadoop execution frame work.
 *
 * There are some pre-defined primitive types in hadoop:
 * @see org.apache.hadoop.io.IntWritable
 * @see org.apache.hadoop.io.LongWritable
 * @see org.apache.hadoop.io.DoubleWritable
 * @see org.apache.hadoop.io.Text
 *
 */
public final class Centroid implements WritableComparable<Centroid> {

	private DoubleVector center;
	private int kTimesIncremented = 1;
	private int clusterIndex;

	public Centroid() {
		super();
	}

	public Centroid(DoubleVector center) {
		super();
		this.center = center.deepCopy();
	}

	public Centroid(Centroid center) {
		super();
		this.center = center.center.deepCopy();
		this.kTimesIncremented = center.kTimesIncremented;
	}

	public Centroid(DataPoint center) {
		super();
		this.center = center.getVector().deepCopy();
	}

	public final void plus(DataPoint c) {
		plus(c.getVector());
	}

	public final void plus(DoubleVector c) {
		center = center.add(c);
		kTimesIncremented++;
	}

	public final void plus(Centroid c) {
		kTimesIncremented += c.kTimesIncremented;
		center = center.add(c.getCenterVector());
	}

	public final int getK() {
		return this.kTimesIncremented;
	}

	public final int divideByK() {
		center = center.divide(kTimesIncremented);
		return kTimesIncremented;
	}

	public final boolean update(Centroid c) {
		return calculateError(c.getCenterVector()) > 0;
	}

	public final double calculateError(DoubleVector v) {
		return Math.sqrt(center.subtract(v).abs().sum());
	}

	@Override
	public final void write(DataOutput out) throws IOException {
		DataPoint.writeVector(center, out);
		out.writeInt(kTimesIncremented);
		out.writeInt(clusterIndex);
	}

	@Override
	public final void readFields(DataInput in) throws IOException {
		this.center = DataPoint.readVector(in);
		kTimesIncremented = in.readInt();
		clusterIndex = in.readInt();
	}

	@Override
	public final int compareTo(Centroid o) {
		return Integer.compare(clusterIndex, o.clusterIndex);
	}

	/**
	 * @return the center
	 */
	public final DoubleVector getCenterVector() {
		return center;
	}

	/**
	 * @return the index of the cluster in a datastructure.
	 */
	public int getClusterIndex() {
		return clusterIndex;
	}

	public void setClusterIndex(int clusterIndex) {
		this.clusterIndex = clusterIndex;
	}

	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((center == null) ? 0 : center.hashCode());
		return result;
	}

	@Override
	public final boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Centroid other = (Centroid) obj;
		if (center == null) {
			if (other.center != null)
				return false;
		} else if (!center.equals(other.center))
			return false;
		return true;
	}

	@Override
	public final String toString() {
		return "Centroid [center=" + center + "]";
	}

}
