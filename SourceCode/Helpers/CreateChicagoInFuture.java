import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class CreateChicagoInFuture {
	
	public static class CreateFileMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		private final String date = "11/30/2014";
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			for(int i = 0; i < 24 ; i++){
				context.write(new Text("input,"+value.toString()+","+i+","+date), NullWritable.get());
			}
		}
		
	}
	
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		
		String[] allArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(allArgs.length < 2){
			System.err.println("Usage ClusterCrimeData <InputFile/InputFolder> <OutputFolder>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Create infuture file");
		job.setJarByClass(CreateChicagoInFuture.class);
		job.setMapperClass(CreateFileMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path(allArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(allArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}
