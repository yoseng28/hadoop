package mapr;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeanScoreMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		System.setProperty("HADOOP_USER_NAME", "root");
		Properties p = new Properties();
		String DATASETS_PATH = p.getProperty("DATASETS_PATH");
		String RESULTS_PATH = p.getProperty("DATASETS_PATH");

		String[] filePaths = { "/meanscore_math.txt", DATASETS_PATH + "/meanscore_python.txt",
				DATASETS_PATH + "meanscore_java.txt", };
		String outputPath = RESULTS_PATH + "/meanscore";

		Job job = Job.getInstance();
		job.setJobName("分数平均成绩");
		job.setJarByClass(MeanScoreMain.class);
		job.setMapperClass(MeanScoreMapper.class);
		job.setCombinerClass(MeanScoreReducer.class);
		job.setReducerClass(MeanScoreReducer.class);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < filePaths.length; i++) {
			FileInputFormat.addInputPath(job, new Path(filePaths[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}

}
