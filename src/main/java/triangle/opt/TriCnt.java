package triangle.opt;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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
		String tmppath3 = inputpath + ".tmp3";
		String tmppath4 = inputpath + ".tmp4";
		String tmppath5 = inputpath + ".tmp5";
		String outpath = inputpath + ".out";
		
		//original
//		runStep1(inputpath, tmppath1);
//		runStep2(inputpath, tmppath1, outpath);
		
		//task 1
//		runPrestep1(inputpath, tmppath1);
//		runStep1(tmppath1, tmppath2);
//		runStep2(tmppath1, tmppath2, outpath);
		
		
		
		// task1 + 2
		runPrestep1(inputpath, tmppath1);
		runPrestep2(tmppath1, tmppath2);
		runPrestep3(tmppath2, tmppath3);
		runPrestep4(tmppath3, tmppath4);
		runStep1(tmppath4, tmppath5);
		runStep2(tmppath4, tmppath5, outpath);
//		
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
	
	private void runPrestep2(String inputpath, String tmppath) throws Exception{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TriCnt.class);
		
		job.setMapperClass(TriPrestep2Mapper.class);
		job.setReducerClass(TriPrestep2Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);//d
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(tmppath));
		
		job.waitForCompletion(true);
		
	}
	
	private void runPrestep3(String inputpath, String tmppath) throws Exception{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TriCnt.class);
		
		job.setMapperClass(TriPrestep3Mapper.class);
		job.setReducerClass(TriPrestep3Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);//d
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(tmppath));
		
		job.waitForCompletion(true);
		
	}	

	private void runPrestep4(String inputpath, String tmppath) throws Exception{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TriCnt.class);
		
		job.setMapperClass(TriPrestep4Mapper.class);
		job.setReducerClass(TriPrestep4Reducer.class);
		
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
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(tmppath));
		
		job.waitForCompletion(true);
		
	}

	private void runStep2(String inputpath, String tmppath, String outpath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TriCnt.class);
		
		job.setReducerClass(TriStep2Reducer.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(TriCntPartitioner.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(inputpath), TextInputFormat.class, TriStep2MapperForEdges.class);
		MultipleInputs.addInputPath(job, new Path(tmppath), SequenceFileInputFormat.class, TriStep2MapperForWedges.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		job.waitForCompletion(true);
		
	}

	

}
