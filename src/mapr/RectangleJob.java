package mapr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RectangleJob {
	
	public static void runJob(String filePath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = Job.getInstance();
		job.setJobName("对正方形和长方形的面积进行排序");
		job.setJarByClass(RectangleJob.class);
		job.setMapperClass(RectangleMapper.class);
		job.setReducerClass(RectangleReducer.class);
		job.setMapOutputKeyClass(RectangleWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(RectanglePartitioner.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(filePath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.out.println("MR开始运行***********************************************");
		long startTime = System.currentTimeMillis();
		boolean result = job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		System.out.println("MR运行时间：" + (endTime - startTime) / 1000 + "秒");
		System.exit(result ? 0 : 1);
	}

}
