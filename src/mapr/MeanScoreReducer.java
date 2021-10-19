package mapr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text name, Iterable<IntWritable> scores, Context context)
			throws IOException, InterruptedException {
		int totalScore = 0;
		int num = 0;
		Iterator<IntWritable> score = scores.iterator();
		while(score.hasNext()) {
			totalScore = totalScore + score.next().get();
			num++;
		}
		// 去掉平均成绩的小数点
		int meanScore = (int) totalScore / num;
		context.write(name, new IntWritable(meanScore));
	}

}
