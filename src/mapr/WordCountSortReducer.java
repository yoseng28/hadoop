package mapr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountSortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text word, Iterable<IntWritable> count, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> it = count.iterator();
		while (it.hasNext()) {
			sum = sum + it.next().get();
		}
		context.write(word, new IntWritable(sum));
	}

}
