import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;



public class ClusterCrimeDataHourly {
	
	/*
	 * PARTITIONER HERE BASED ON ZIP CODE - All crimes with the same Zip Code are sent to the same Reducer
	 */
	public static class ClusterPartitioner extends Partitioner<Text, Text>{
		CSVParser csvParser;
		
		@Override
		public int getPartition(Text key, Text value, int numOfTasks) {
			if(numOfTasks == 0)
				return 0;
			
			return Math.abs(key.toString().hashCode())%numOfTasks;
		}
	}
	
	
	public static class ClusterData{
		int[] monthWiseData;
		int[] hourWiseData;
		
		public ClusterData(){
			monthWiseData = new int[]{0,0,0,0,0,0,0,0,0,0,0,0};
			hourWiseData = new int[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
		}

		public void addToMonth(int month){
			this.monthWiseData[month-1] += 1;
		}
		
		public void addToHour(int hour){
			this.hourWiseData[hour] += 1;
		}
	}
	
	
	public static class ClusterMapper extends Mapper<LongWritable, Text, Text, Text>{
		private CSVParser tabParser;
		private CSVParser csvParser;
		private CSVParser atParser;
		private CSVParser dollarParser;
		private final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
		private Date maxDate;
		private final double weightOfMonthWise = 20.0;
		private final double weightOfRecent = 50.0;
		private final double weightOfHourWise = 75.0;
		
		@SuppressWarnings("deprecation")
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			tabParser = new CSVParser('\t');
			csvParser = new CSVParser();
			atParser = new CSVParser('@');
			dollarParser = new CSVParser('$');
			maxDate = new Date("11/15/2014 00:00");
		}
		
		@SuppressWarnings("deprecation")
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			HashMap<String, ClusterData> zipHourToClusterData = new HashMap<String, ClusterData>();
			List<String> keyValuePairs = Arrays.asList(tabParser.parseLine(value.toString()));
			List<String> keys = Arrays.asList(csvParser.parseLine(keyValuePairs.get(0)));
			List<String> values = Arrays.asList(atParser.parseLine(keyValuePairs.get(1)));
			
			int totalCount =0;
			int recentCount=0;
			
			for(String eachCrime : values){
				List<String> crimeAttribs = Arrays.asList(csvParser.parseLine(eachCrime));
				
				totalCount += Integer.valueOf(crimeAttribs.get(1));
				
				List<String> allDates = Arrays.asList(dollarParser.parseLine(crimeAttribs.get(2)));
				
				for(String eachDate : allDates){
					Date dt = new Date(eachDate);
					int hour = dt.getHours();
					int month = dt.getMonth() + 1;
					
					int diffInDays = (int) ((maxDate.getTime() - dt.getTime())/ DAY_IN_MILLIS );
					if(diffInDays <= 365)
						recentCount++;
					
					String mapKey = keys.get(3) + "," + hour;
					if(zipHourToClusterData.containsKey(mapKey)){
						zipHourToClusterData.get(mapKey).addToMonth(month);
						zipHourToClusterData.get(mapKey).addToHour(hour);
					}
					else{
						ClusterData clstrData = new ClusterData();
						clstrData.addToMonth(month);
						clstrData.addToHour(hour);
						zipHourToClusterData.put(mapKey, clstrData);
					}
				}
			}
			
			for(String mapKey : zipHourToClusterData.keySet()){
				int getHour = Integer.parseInt(mapKey.split(",")[1]);
				double totalPlusRecentWeight = totalCount + (recentCount * weightOfRecent);
				
				for(int i = 0; i < zipHourToClusterData.get(mapKey).monthWiseData.length ; i++){
					double totalWeight = 0.0;
					if(zipHourToClusterData.get(mapKey).monthWiseData[i] == 0){
						totalWeight += totalCount;
						if(zipHourToClusterData.get(mapKey).hourWiseData[getHour] != 0){
							totalWeight += (recentCount * weightOfRecent) + zipHourToClusterData.get(mapKey).hourWiseData[getHour] * weightOfHourWise;
						}
					}
					else{
						totalWeight += totalPlusRecentWeight + (zipHourToClusterData.get(mapKey).monthWiseData[i] * weightOfMonthWise);
						if(zipHourToClusterData.get(mapKey).hourWiseData[getHour] != 0){
							totalWeight += zipHourToClusterData.get(mapKey).hourWiseData[getHour] * weightOfHourWise;
						}
					}
					
					context.write(new Text(mapKey+","+ (i+1)),
							  new Text(keys.get(0) + ":" + totalWeight));
				}
				
			}
			
			
		}
	}
	
	public static class ClusterGroupingComparator extends WritableComparator{

		protected ClusterGroupingComparator() {
			super(Text.class,true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable compositeKey1, WritableComparable compositeKey2) {
			
			Integer zipHourMonthHashCode1,zipHourMonthHashCode2;
			
			try{
				zipHourMonthHashCode1 = Math.abs(compositeKey1.toString().hashCode());
				zipHourMonthHashCode2 = Math.abs(compositeKey2.toString().hashCode());
				
			}catch(Exception e){return -1;}
			
			return zipHourMonthHashCode1.compareTo(zipHourMonthHashCode2);
		}
	}
	
	public static class WeightComparator implements Comparator<String>{
		private CSVParser colParser;
		
		@Override
		public int compare(String streetWeight1, String streetWeight2) {
			colParser = new CSVParser(':');
			Double weight1 = null,weight2 = null;
			try {
				weight1 = Double.parseDouble(colParser.parseLine(streetWeight1)[1]);
				weight2 = Double.parseDouble(colParser.parseLine(streetWeight2)[1]);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			return weight1.compareTo(weight2) * -1;
		}
	}
	
	public static class ClusterReducer extends Reducer<Text, Text, Text, Text>{
		
		private String buildString(ArrayList<String> valuesToWrite){
			StringBuilder valueString = new StringBuilder();
			
			for(int i = 0; i< valuesToWrite.size() ; i++){
				if(i != 0)
					valueString.append("@"+valuesToWrite.get(i));
				else
					valueString.append(valuesToWrite.get(i));
			}
			
			return valueString.toString();
		}
		
		
		@Override
		protected void reduce(Text zipHourMonth, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			ArrayList<String> valuesToWrite = new ArrayList<String>();
			
			for(Text eachVal : values)
				valuesToWrite.add(eachVal.toString());
			
			Collections.sort(valuesToWrite, new WeightComparator());
			
			context.write(new Text(zipHourMonth), new Text(buildString(valuesToWrite)));
		}
	}
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		
		String[] allArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(allArgs.length < 2){
			System.err.println("Usage ClusterCrimeData <InputFile/InputFolder> <OutputFolder>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Cluster Crime Graph");
		job.setJarByClass(ClusterCrimeData.class);
		job.setMapperClass(ClusterMapper.class);
		job.setGroupingComparatorClass(ClusterGroupingComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(5);
		job.setPartitionerClass(ClusterPartitioner.class);
		job.setReducerClass(ClusterReducer.class);
		FileInputFormat.addInputPath(job, new Path(allArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(allArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}

}
