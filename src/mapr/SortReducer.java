package mapr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	// 全局变量
	private static int count = 0;

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		for (@SuppressWarnings("unused")
		IntWritable v : values) {
			count = count + 1;
			context.write(new IntWritable(count), key);
		}
	}
}
