package triangle;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TriCnt extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TriCnt(), args);
	}
	
	public int run(String[] args) throws Exception {
		
		String inputpath = args[0];
		String tmppath1 = inputpath + ".tmp1";
		String tmppath2 = inputpath + ".tmp2";
		String outpath = inputpath + ".out";
		
		
		//runPrestep1(inputpath, tmppath1);
		runStep1(inputpath, tmppath2);
		runStep2(inputpath, tmppath2, outpath);
		
		return 0;
	}
	private void runPrestep1(String inputpath, String tmppath) throws Exception{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TriCnt.class);
		
		job.setMapperClass(TriPrestep1Mapper.class);
		job.setReducerClass(TriPrestep1Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);//d
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(tmppath));
		
		job.waitForCompletion(true);
		
	}
	private void runStep1(String inputpath, String tmppath) throws Exception{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TriCnt.class);
		
		job.setMapperClass(TriStep1Mapper.class);
		job.setReducerClass(TriStep1Reducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);//d
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(tmppath));
		
		job.waitForCompletion(true);
		
	}

	private void runStep2(String inputpath, String tmppath, String outpath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TriCnt.class);
		
		job.setMapperClass(TriStep2Mapper.class);
		job.setReducerClass(TriStep2Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileInputFormat.addInputPath(job, new Path(tmppath));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		job.waitForCompletion(true);
		
	}

	

}
