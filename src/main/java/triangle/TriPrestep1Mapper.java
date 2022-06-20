package triangle;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriPrestep1Mapper extends Mapper<Object, Text, Text, IntWritable> {
	
	Text ok = new Text();
	IntWritable ov = new IntWritable();
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
		
		int u = Integer.parseInt(st.nextToken());
		int v = Integer.parseInt(st.nextToken());
		if (u != v) {
			if (u < v) {
				ok.set(u +" " + v);
				ov.set(-1);
				context.write(ok, ov);
			}
			else if (u > v) {
				ok.set(v +" " + u);
				ov.set(-1);
				context.write(ok, ov);
			}
		}	
	}
}
