package mapr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GansuAirportsReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
			@SuppressWarnings("unused")
			// <兰州市,｛"", "中川机场"｝>
			// <白银市,｛""｝>
			Text value = iterator.next();
			count++;
		}
		if (count == 1) {
			context.write(new Text(key), new Text(""));
		}
	}
}
