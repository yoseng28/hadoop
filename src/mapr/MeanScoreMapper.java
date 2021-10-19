package mapr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeanScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer lines = new StringTokenizer(value.toString(), "\n");
		while (lines.hasMoreElements()) {
			StringTokenizer line = new StringTokenizer(lines.nextToken());
			String name = line.nextToken();
			int score = Integer.parseInt(line.nextToken());
			context.write(new Text(name), new IntWritable(score));
		}
	}

}
