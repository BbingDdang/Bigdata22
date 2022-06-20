package triangle;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TriPrestep2Reducer extends Reducer<IntWritable, IntWritable, Text, Text>{
	Text ok = new Text();
	Text ov = new Text();
	
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Reducer<IntWritable, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		
		int cnt = 0;
		ArrayList<Integer> neighbors = new ArrayList<Integer>();
		for(IntWritable v : values) {
			cnt += 1;
		}
		
//		ov.set("" + key.get());
//		
//		for(int u : neighbors) {
//			for(int v : neighbors) {
//				if (u < v) {
//					ok.set(u + "\t" + v);
//					context.write(ok, ov);
//				}
//			}
//		}
		ok.set(key.toString());
		
	}
}
