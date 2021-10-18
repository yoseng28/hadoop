package mapr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// context是reduce的上下文
		int sum = 0;
		for (IntWritable v : values) {
			sum = sum + v.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
