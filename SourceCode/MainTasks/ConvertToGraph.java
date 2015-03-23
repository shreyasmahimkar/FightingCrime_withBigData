import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class ConvertToGraph {
	
	
	public static class GraphMapper extends Mapper<LongWritable, Text, Text, Text>{
		private CSVParser csvParser;
		private int CRIME_STREET_COLUMN = 0;
		private int CRIME_TIME_COLUMN = 1;
		private int CRIME_TYPE_COLUMN = 2;
		private int CRIME_CITY_COLUMN = 3;
		private int CRIME_STATE_COLUMN = 4;
		private int CRIME_ZIPCODE_COLUMN =5;
		
		private boolean isRecordValid(List<String> inputRecord){
			if(isBlankOrNull(inputRecord.get(CRIME_STREET_COLUMN))
					|| isBlankOrNull(inputRecord.get(CRIME_CITY_COLUMN))
					|| isBlankOrNull(inputRecord.get(CRIME_STATE_COLUMN))
					|| isBlankOrNull(inputRecord.get(CRIME_TIME_COLUMN))
					|| isBlankOrNull(inputRecord.get(CRIME_ZIPCODE_COLUMN))
					|| isBlankOrNull(inputRecord.get(CRIME_TYPE_COLUMN)))
				return false;
			
			return true;
		}
		
		// Helper Function to check if any of the column values are null or blank
		private boolean isBlankOrNull(Object anyObj){
			if(anyObj == null)
				return true;
			if(anyObj.equals(""))
				return true;
			
			return false;
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			csvParser = new CSVParser();
			List<String> inputRecord = Arrays.asList(csvParser.parseLine(value.toString()));
			
			if(isRecordValid(inputRecord)){
				CrimeLocationNode locationNode = new CrimeLocationNode(inputRecord.get(CRIME_STREET_COLUMN).toLowerCase(), 
																	   inputRecord.get(CRIME_CITY_COLUMN).toLowerCase(), 
																	   inputRecord.get(CRIME_STATE_COLUMN).toLowerCase(), 
																	   inputRecord.get(CRIME_ZIPCODE_COLUMN).toLowerCase());
				
				
				context.write(new Text(locationNode.createTextForNode()), 
							  new Text(inputRecord.get(CRIME_TYPE_COLUMN)+","+inputRecord.get(CRIME_TIME_COLUMN)));
			}
		}
	}
	
	/*
	 * PARTITIONER HERE BASED ON ZIP CODE - All crimes with the same Zip Code are sent to the same Reducer
	 */
	public static class GraphPartitioner extends Partitioner<Text, Text>{
		CSVParser csvParser;
		
		@Override
		public int getPartition(Text key, Text value, int numOfTasks) {
			if(numOfTasks == 0)
				return 0;
			String zipCode =null;
			try {
				zipCode = new CSVParser().parseLine(key.toString())[3];
			} catch (IOException e) {e.printStackTrace();} 
			
			if(zipCode == null)
				return 0;
			
			return Math.abs(zipCode.hashCode())%numOfTasks;
		}
	}
	
	
	
	public static class GraphReducer extends Reducer<Text, Text, Text, Text>{
		private HashMap<String, HashMap<String, CrimeNode>> crimeLocationToCrimeTypesMap;
		private CSVParser csvParser;
		@Override
		protected void setup(
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			crimeLocationToCrimeTypesMap = new HashMap<String, HashMap<String, CrimeNode>>();
			csvParser = new CSVParser();
		}
		
		@Override
		protected void reduce(Text locationOfCrime, Iterable<Text> allCrimeTypeAndTime, Reducer<Text, Text, Text, Text>.Context context)
														throws IOException, InterruptedException {
			CrimeNode crimeNode;
			
			for(Text crimeTypeAndTime : allCrimeTypeAndTime){
				List<String> crimeTypeAndTimeSplit = Arrays.asList(csvParser.parseLine(crimeTypeAndTime.toString().toLowerCase()));
				HashMap<String,CrimeNode> crimeNodeMap;
				
				if((crimeNodeMap = crimeLocationToCrimeTypesMap.get(locationOfCrime.toString()))!=null){
					
					if((crimeNode = crimeNodeMap.get(crimeTypeAndTimeSplit.get(0))) != null){
						crimeNode.addCount();
						crimeNode.addTime(crimeTypeAndTimeSplit.get(1));
					}
					else{
						crimeNode = new CrimeNode(crimeTypeAndTimeSplit.get(0), crimeTypeAndTimeSplit.get(1));
						crimeNodeMap.put(crimeTypeAndTimeSplit.get(0), crimeNode);
					}
				}
				else{
					crimeNode = new CrimeNode(crimeTypeAndTimeSplit.get(0), crimeTypeAndTimeSplit.get(1));
					HashMap<String, CrimeNode> initialCrimeNodeMap = new HashMap<String, CrimeNode>();
					initialCrimeNodeMap.put(crimeTypeAndTimeSplit.get(0), crimeNode);
					crimeLocationToCrimeTypesMap.put(locationOfCrime.toString(), initialCrimeNodeMap);
				}
			}
		}
		
		@Override
		protected void cleanup(
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String delimiterBetweenCrimeTypes = "@";
			int countOfInnerLoop = 0;
			for(String location : crimeLocationToCrimeTypesMap.keySet()){
				HashMap<String, CrimeNode> crimeNodeMap = crimeLocationToCrimeTypesMap.get(location);
				String valueString = "";
				for(String crimeType : crimeNodeMap.keySet()){
					if(countOfInnerLoop == 0)
						valueString += crimeNodeMap.get(crimeType).createTextForNode();
					else
						valueString += delimiterBetweenCrimeTypes + crimeNodeMap.get(crimeType).createTextForNode();
					
					countOfInnerLoop++;
				}
				context.write(new Text(location), new Text(valueString));
				countOfInnerLoop = 0;
			}
		}
		
	}
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		
		String[] allArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(allArgs.length < 2){
			System.err.println("Usage convertToGraph <InputFile/InputFolder> <OutputFolder>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Convert Crime Data To Graph");
		job.setJarByClass(ConvertToGraph.class);
		job.setMapperClass(GraphMapper.class);
		job.setReducerClass(GraphReducer.class);
		job.setPartitionerClass(GraphPartitioner.class);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(allArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(allArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}
