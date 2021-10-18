package mapr;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tools.HDFSTools;

public class SortJobMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "root");
		String[] filesPath = { "hdfs://192.168.184.3:8020/datasets/sort1.txt",
				"hdfs://192.168.184.3:8020/datasets/sort2.txt" };
		String outputPath = "hdfs://192.168.184.3:8020/results/sort";

		Job job = Job.getInstance();
		job.setJobName("数字排序");
		job.setJarByClass(SortJobMain.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		 
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		for(String path:filesPath) {
			FileInputFormat.addInputPath(job, new Path(path));
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
