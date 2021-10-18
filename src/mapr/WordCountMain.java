package mapr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountMain {

	/* 本事例通过args传递参数*/
	// hdfs://192.168.184.3:8020/datasets/news1.txt
	// hdfs://192.168.184.3:8020/datasets/news2.txt
	// hdfs://192.168.184.3:8020/results/wordcount
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		System.setProperty("HADOOP_USER_NAME", "root");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		WordCountJob.jobConfig(otherArgs);
	}
}
