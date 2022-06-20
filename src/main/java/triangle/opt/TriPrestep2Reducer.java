package triangle.opt;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TriPrestep2Reducer extends Reducer<Text, Text, Text, Text>{
	Text ok = new Text();
	Text ov = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		int cnt = 0;
		
		ArrayList<String> neighbors = new ArrayList<String>();
		for(Text v : values) {
			cnt += 1;
			
			neighbors.add(v.toString());
		}
		
		
		//System.out.println(neighbors);
		ok.set(Integer.toString(cnt));

		for(String n : neighbors) {
			ov.set(n);
			//System.out.println(n);
			context.write(ov, ok);
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
		
		
	}
}
