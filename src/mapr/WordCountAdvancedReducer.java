package mapr;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountAdvancedReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

	// 树形结构的Map集合，会对传入的key进行了大小排序
	private TreeMap<Integer, String> sortedWords;

	@Override
	protected void setup(Context context) {
		sortedWords = new TreeMap<>(java.util.Collections.reverseOrder());
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum = sum + value.get();
		}
		if (sortedWords.containsKey(sum)) {
			// 具有相同个数的单词用逗号隔开，放置被覆盖掉
			String words = sortedWords.get(sum) + "," + key.toString();
			sortedWords.put(sum, words);
		} else {
			sortedWords.put(sum, key.toString());
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<Integer, String> entry : sortedWords.entrySet()) {
			context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
		}
	}
}