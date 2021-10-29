package mapr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class WordCountSortMain {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		System.setProperty("HADOOP_USER_NAME", "root");
		String[] filePath = { "hdfs://192.168.184.3:8020/datasets/news1.txt",
				"hdfs://192.168.184.3:8020/datasets/news2.txt" };
		String outputPath = "hdfs://192.168.184.3:8020/results/wordcount_sort";
		String tmpPath = "hdfs://192.168.184.3:8020/results/tmp";
		WordCountSortJob.jobConfig(filePath, outputPath, tmpPath);
	}
}
