package triangle.opt;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TriPrestep1Reducer extends Reducer<Text, IntWritable, Text, Text>{
	Text ok = new Text();
	Text ov = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		
		//ArrayList<Integer> neighbors = new ArrayList<Integer>();
//		for(IntWritable v : values) {
//			neighbors.add(v.get());
//		}
		
		context.write(key, ov);
		
//		for(int u : neighbors) {
//			for(int v : neighbors) {
//				if (u < v) {
//					ok.set(u + "\t" + v);
//					context.write(ok, ov);
//				}
//			}
//		}
		
	}
}
