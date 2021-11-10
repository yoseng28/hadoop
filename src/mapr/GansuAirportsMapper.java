package mapr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GansuAirportsMapper {

	static class SomeCityMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split(" ");
			// ｛兰州市, 中川机场｝
			context.write(new Text(lines[0]), new Text(lines[1]));
		}
	}

	static class AllCityMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String cityName = value.toString();
			// ｛兰州市 , ""｝
			// ｛白银市 , ""｝
			context.write(new Text(cityName), new Text(""));
		};
	}

}
