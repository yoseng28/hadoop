package mapr;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapr.GansuAirportsMapper.AllCityMapper;
import mapr.GansuAirportsMapper.SomeCityMapper;
import tools.HDFSTools;

public class GansuAirportsMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		System.setProperty("HADOOP_USER_NAME", "root");
		String[] filesPath = { "hdfs://192.168.184.3:8020/datasets/gansu_airports.txt",
				"hdfs://192.168.184.3:8020/datasets/gansu.txt" };
		String outputPath = "hdfs://192.168.184.3:8020/results/gansu_airports";

		Job job = Job.getInstance();
		job.setJobName("甘肃未建机场城市");
		job.setJarByClass(GansuAirportsMain.class);
		MultipleInputs.addInputPath(job, new Path(filesPath[0]), TextInputFormat.class, SomeCityMapper.class);
		MultipleInputs.addInputPath(job, new Path(filesPath[1]), TextInputFormat.class, AllCityMapper.class);
		try {
			HDFSTools.deleteFileAndDir(outputPath);
			HDFSTools.closeFileSystem();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setReducerClass(GansuAirportsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.out.println("MR开始运行***********************************************");
		long startTime = System.currentTimeMillis();
		boolean result = job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		System.out.println("MR运行时间：" + (endTime - startTime) / 1000 + "秒");
		System.exit(result ? 0 : 1);
	}
}
