package mapr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RectangleReducer extends Reducer<RectangleWritable, NullWritable, IntWritable, IntWritable> {
	public void reduce(RectangleWritable key, Iterable<NullWritable> value, Context context)
			throws IOException, InterruptedException {
		context.write(new IntWritable(key.getLength()), new IntWritable(key.getWidth()));
	}
}
