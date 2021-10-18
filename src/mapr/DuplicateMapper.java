package mapr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DuplicateMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		Text line = value;
		context.write(line, new Text(""));
	}

}
