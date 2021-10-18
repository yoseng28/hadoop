package mapr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RectanglePartitioner extends Partitioner<RectangleWritable, NullWritable> {

	@Override
	public int getPartition(RectangleWritable rw, NullWritable arg1, int arg2) {
		if (rw.getLength() == rw.getWidth()) {
			// 正方形在任务0中汇总
			return 0;
		}
		// 长方形在任务1中汇总
		return 1;
	}

}
