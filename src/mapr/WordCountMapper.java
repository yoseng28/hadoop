package mapr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// context是Mapper的上下文
		String tmp_word;
		StringTokenizer token = new StringTokenizer(value.toString());
		while (token.hasMoreTokens()) {
			tmp_word = token.nextToken();
			// StringTokenizer默认的分隔符是空格(" ")、制表符(\t)、换行符(\n)、回车符(\r)
			if (tmp_word.startsWith("\"")) {
				tmp_word = tmp_word.substring(1, tmp_word.length());
			}
			if (tmp_word.endsWith("\"")) {
				tmp_word = tmp_word.substring(0, tmp_word.length()-1);
			}
			if (tmp_word.endsWith(",") | tmp_word.endsWith(".")) {
				tmp_word = tmp_word.substring(0, tmp_word.length() - 1);
			}
			context.write(new Text(tmp_word), new IntWritable(1));
		}
	}

}
