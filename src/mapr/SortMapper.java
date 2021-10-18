package mapr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(" ");
		context.write(new IntWritable(Integer.parseInt(values[1])), new IntWritable(1));
	}
}
