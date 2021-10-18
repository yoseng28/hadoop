package mapr;

import java.io.IOException;

// import org.apache.hadoop.conf.Configuration;

public class RectangleMain {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// Configuration conf = new Configuration();
		// conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		System.setProperty("HADOOP_USER_NAME", "root");
		String filePath = "hdfs://192.168.184.3:8020/datasets/rectangle.txt";
		String resultPath ="hdfs://192.168.184.3:8020/results/rectangle";
		RectangleJob.runJob(filePath,resultPath);
		
	}

}
