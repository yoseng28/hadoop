package mapr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountSortMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		while (st.hasMoreTokens()) {
			String word = st.nextToken();
			// StringTokenizer默认的分隔符是空格(" ")、制表符(\t)、换行符(\n)、回车符(\r)
			if (word.startsWith("\"")) {
				word = word.substring(1, word.length());
			}
			if (word.endsWith("\"")) {
				word = word.substring(0, word.length() - 1);
			}
			if (word.endsWith(",") | word.endsWith(".")) {
				word = word.substring(0, word.length() - 1);
			}
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
