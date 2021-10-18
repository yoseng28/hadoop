package mapr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapper extends Mapper<LongWritable, Text, RectangleWritable, NullWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(" ");
		RectangleWritable rw = new RectangleWritable(Integer.parseInt(values[0]), Integer.parseInt(values[1]));
		context.write(rw, NullWritable.get());
	}
}
