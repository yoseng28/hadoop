package mapr;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ResourceBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tools.HDFSTools;

public class WordCountAdvancedMain {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		System.setProperty("HADOOP_USER_NAME", "root");
		ResourceBundle resource = ResourceBundle.getBundle("conf");
		String DATASETS_PATH = resource.getString("DATASETS_PATH");
		String RESULTS_PATH = resource.getString("RESULTS_PATH");
		String[] filesPath = { DATASETS_PATH + "/news1.txt", DATASETS_PATH + "/news2.txt" };
		String outputPath = RESULTS_PATH + "/wordcount_adv";

		Job job = Job.getInstance();
		job.setJarByClass(WordCountAdvancedMain.class);
		job.setMapperClass(WordCountAdvancedMapper.class);
		job.setReducerClass(WordCountAdvancedReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (String fp : filesPath) {
			FileInputFormat.addInputPath(job, new Path(fp));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		// 输出目录如果存在，删除
		try {
			HDFSTools.getFileSystem();
			HDFSTools.deleteFileAndDir(outputPath);
			HDFSTools.closeFileSystem();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		System.out.println("MapReduce开始运行！");
		long startTime = System.currentTimeMillis();
		boolean result = job.waitForCompletion(true);
		if (result) {
			long endTime = System.currentTimeMillis();
			System.out.println("MR运行时间：" + (endTime - startTime) / 1000 + "秒");
			System.out.println("MapReduce结束运行！");
		}
		
	}

}
