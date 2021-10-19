package mapr;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ResourceBundle;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tools.HDFSTools;

public class MeanScoreMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		System.setProperty("HADOOP_USER_NAME", "root");

		ResourceBundle resource = ResourceBundle.getBundle("conf");
		String DATASETS_PATH = resource.getString("DATASETS_PATH");
		String RESULTS_PATH = resource.getString("RESULTS_PATH");

		String[] filePaths = { DATASETS_PATH + "/meanscore_math.txt", DATASETS_PATH + "/meanscore_python.txt",
				DATASETS_PATH + "/meanscore_java.txt", };
		String outputPath = RESULTS_PATH + "/meanscore";

		Job job = Job.getInstance();
		job.setJobName("分数平均成绩");
		job.setJarByClass(MeanScoreMain.class);
		job.setMapperClass(MeanScoreMapper.class);
		job.setCombinerClass(MeanScoreReducer.class);
		job.setReducerClass(MeanScoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < filePaths.length; i++) {
			FileInputFormat.addInputPath(job, new Path(filePaths[i]));
		}
		try {
			// 输出目录如果存在，删除
			HDFSTools.deleteFileAndDir(outputPath);
			HDFSTools.closeFileSystem();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		System.out.println("MR开始运行***********************************************");
		long startTime = System.currentTimeMillis();
		boolean result = job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		System.out.println("MR运行时间：" + (endTime - startTime) / 1000 + "秒");
		System.exit(result ? 0 : 1);
	}

}
