package triangle;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TriStep2Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	IntWritable ov = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		boolean edge_exists = false; 
		ArrayList<Integer> nodes = new ArrayList<Integer>();

		for(IntWritable v : values) {
			if(v.get() == -1) {
				edge_exists = true;
			}
			else {
				nodes.add(v.get());
			}
		}
		
		if(edge_exists) {
			for(int v : nodes) {
				ov.set(v);
				context.write(key, ov);
			}
		}
	}
}
