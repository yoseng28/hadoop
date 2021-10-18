package mapr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DuplicateMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "root");
		String [] filePaths = {"hdfs://192.168.184.3:8020/datasets/duplicate1.txt",
		                      "hdfs://192.168.184.3:8020/datasets/duplicate2.txt"};
		String outputPath = "hdfs://192.168.184.3:8020/results/duplicate";
		Job job = Job.getInstance();
		job.setJobName("数据去重");
		job.setJarByClass(DuplicateMain.class);
		job.setMapperClass(DuplicateMapper.class);
		job.setReducerClass(DuplicateReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		for(int i=0;i<filePaths.length;i++) {
			FileInputFormat.addInputPath(job, new Path(filePaths[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
		
	}

}
