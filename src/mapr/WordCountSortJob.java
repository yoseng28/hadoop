package mapr;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import tools.HDFSTools;

public class WordCountSortJob {

	public static void jobConfig(String[] filePath, String outputPath, String tmpPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		// 创建job和任务入口
		// job：map函数和reduce函数组织起来的组件
		Job job = Job.getInstance();
		job.setJobName("词频统计&降序");
		job.setJarByClass(WordCountSortJob.class);
		job.setMapperClass(WordCountSortMapper.class);
		job.setCombinerClass(WordCountSortReducer.class);
		job.setReducerClass(WordCountSortReducer.class);

		// 设置reduce输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置第一个job的输出格式
		// 默认输出格式：TextOutputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// 设置分区数
		job.setNumReduceTasks(2);

		// 指定job输入输出目录
		for (int i = 0; i < filePath.length; i++) {
			FileInputFormat.addInputPath(job, new Path(filePath[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(tmpPath));

		// 输出目录如果存在，删除
		try {
			HDFSTools.deleteFileAndDir(tmpPath);
			HDFSTools.deleteFileAndDir(outputPath);
			HDFSTools.closeFileSystem();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		// 执行job
		System.out.println("MR开始运行***********************************************");
		long startTime = System.currentTimeMillis();
		boolean result = job.waitForCompletion(true);

		// 执行倒序排序job
		if (result) {
			Job sortJob = Job.getInstance();
			sortJob.setJarByClass(WordCountSortJob.class);
			FileInputFormat.addInputPath(sortJob, new Path(tmpPath));
			FileOutputFormat.setOutputPath(sortJob, new Path(outputPath));
			// 交换Map中的key、value
			sortJob.setMapperClass(InverseMapper.class);
			sortJob.setNumReduceTasks(1);
			// 设置reduce输出类型
			sortJob.setOutputKeyClass(IntWritable.class);
			sortJob.setOutputValueClass(Text.class);

			// 设置第二个job的输入格式，要与第一个job的输出格式保持一致
			// 统一设置为SequenceFile
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);
			// 实现倒序排序
			sortJob.setSortComparatorClass(WordCountSortComparator.class);
			boolean final_result = sortJob.waitForCompletion(true);
			if (final_result) {
				long endTime = System.currentTimeMillis();
				System.out.println("MR运行时间：" + (endTime - startTime) / 1000 + "秒");
			}
			System.exit(final_result ? 0 : 1);
		}
	}

}
