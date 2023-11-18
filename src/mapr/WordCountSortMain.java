package mapr;

import java.io.IOException;
import java.util.ResourceBundle;

import org.apache.hadoop.conf.Configuration;

public class WordCountSortMain {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		System.setProperty("HADOOP_USER_NAME", "root");
		ResourceBundle resource = ResourceBundle.getBundle("conf");
		String DATASETS_PATH = resource.getString("DATASETS_PATH");
		String RESULTS_PATH = resource.getString("RESULTS_PATH");
		String[] filePath = { DATASETS_PATH + "/news1.txt", DATASETS_PATH + "/news2.txt" };
		String outputPath = RESULTS_PATH + "/wordcount_sort";
		String tmpPath = RESULTS_PATH + "/tmp";
		WordCountSortJob.jobConfig(filePath, outputPath, tmpPath);
	}
}
