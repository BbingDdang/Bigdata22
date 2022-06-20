package triangle.opt;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriPrestep2Mapper extends Mapper<Object, Text, Text, Text> {
	
	Text ok = new Text();
	Text ov = new Text();
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
		
		int u = Integer.parseInt(st.nextToken());
		int v = Integer.parseInt(st.nextToken());
		
		ok.set(Integer.toString(u));
		ov.set(u + " " + v);
		context.write(ok, ov);
//		if (u < v) {
//			ok.set(u);
//			ov.set(v);
//			context.write(ok, ov);
//		}
//		else if (u > v) {
//			ok.set(v);
//			ov.set(u);
//			context.write(ok, ov);
//		}
	}
}
