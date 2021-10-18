package mapr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalaryTotalMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	@Override
	protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
		String data = value1.toString();
		String[] info = data.split(",");

		// 按照索引位置取部门号和薪水
		context.write(new IntWritable(Integer.parseInt(info[7])), new IntWritable(Integer.parseInt(info[5])));
	}

}
