import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.neighboursearch.KDTree;
import au.com.bytecode.opencsv.CSVParser;

public class kdtree_knn {

  public static class knnMapper extends Mapper<Object, Text, Text, Text> {
    Text all = new Text();
    CSVParser csvptab = new CSVParser('\t');
    String[] key_val = null;
    CSVParser csvp = new CSVParser();
    CSVParser csvpat = new CSVParser('@');
    CSVParser csvpcol = new CSVParser(':');

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      // Using CSVParser to parse the value
      String[] tokens = null;

      try {
        tokens = csvp.parseLine(value.toString());
        if (tokens[0].equalsIgnoreCase("input")) {

          // key = input,zip :: value = zip,hour,date
          context.write(new Text(tokens[0] + "," + tokens[1]), new Text(
              tokens[1] + "," + tokens[2] + "," + tokens[3]));
        } else {

          key_val = csvptab.parseLine(value.toString());
          all.set(key_val[0]);
          if (null != key_val && key_val.length == 2) {

            String[] toks = csvpat.parseLine(key_val[1]);
            for (String val : toks) {
              context.write(all, new Text(val.toString()));
            }
          }
        }
      } catch (IOException io) {

      }
    }
  }
  // Custom Partitioner to send same uniquecarrier to same reduce task
  public static class customZipPartitioner extends Partitioner<Text, Text> {
    CSVParser csvp = new CSVParser();

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
      // get the zip and partition based on zip

      // return 0;
      String[] tokens = null;
      try {
        tokens = csvp.parseLine(key.toString());
        if (tokens[0].equalsIgnoreCase("input")) {
          return tokens[1].hashCode() % numReduceTasks;

        }
      } catch (IOException e) { // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return tokens[0].hashCode() % numReduceTasks;
    }
  }

  public static class knnReducer extends Reducer<Text, Text, Text, Text> {
    Attribute Attribute1 = null;
    Attribute Attribute2 = null;
    Attribute Attribute3 = null;
    Attribute Attribute4 = null;
    // Declare the feature vector
    FastVector fvWekaAttributes = null;
    Instances isTrainingSet = null;
    CSVParser csvp = new CSVParser();
    CSVParser csvpdate = new CSVParser('/');
    CSVParser csvpat = new CSVParser('@');
    KDTree myKDTree;
    LinkedHashMap<String, String> h = new LinkedHashMap<String, String>();

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
      Attribute1 = new Attribute("firstNumeric");
      Attribute2 = new Attribute("secondNumeric");
      Attribute3 = new Attribute("thirdNumeric");
      Attribute4 = new Attribute("fourthString", (FastVector) null);
      fvWekaAttributes = new FastVector(4);
      fvWekaAttributes.addElement(Attribute1);
      fvWekaAttributes.addElement(Attribute2);
      fvWekaAttributes.addElement(Attribute3);
      fvWekaAttributes.addElement(Attribute4);
      isTrainingSet = new Instances("Rel", fvWekaAttributes, 1);
      isTrainingSet.setClassIndex(3);
    }

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String[] tokens = csvp.parseLine(key.toString());

      if (tokens[0].equalsIgnoreCase("input")) {
        for (Text val : values) {
          h.put(val.toString(), "");
        }
      } else {

        for (Text val : values) {
          Instance iExample = new Instance(4);
          iExample.setValue((Attribute) fvWekaAttributes.elementAt(0),
              Integer.parseInt(tokens[0]));
          iExample.setValue((Attribute) fvWekaAttributes.elementAt(1),
              Integer.parseInt(tokens[1]));
          iExample.setValue((Attribute) fvWekaAttributes.elementAt(2),
              Integer.parseInt(tokens[2]));
          iExample.setValue((Attribute) fvWekaAttributes.elementAt(3),
              val.toString());
          isTrainingSet.add(iExample);
        }
      }
    }

    public static class WeightComparator implements Comparator<String> {
      private CSVParser colParser;

      @Override
      public int compare(String streetWeight1, String streetWeight2) {
        colParser = new CSVParser(':');
        Double weight1 = null, weight2 = null;
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

    private String buildString(List<String> lofst) {
      StringBuilder valueString = new StringBuilder();

      for (int i = 0; i < lofst.size(); i++) {
        if (i != 0)
          valueString.append("," + lofst.get(i));
        else
          valueString.append(lofst.get(i));
      }

      return valueString.toString();
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      myKDTree = new KDTree();
      try {
        myKDTree.setInstances(isTrainingSet);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      Instance inst = null, output = null;
      Instances outputs = null;
      for (Entry<String, String> e1 : h.entrySet()) {
        // hs =new HashSet<String>();
        String[] tokens = csvp.parseLine(e1.getKey());
        String[] dtkns = csvpdate.parseLine(tokens[2]);
        inst = new Instance(3);
        inst.setDataset(isTrainingSet);
        inst.setValue(0, Integer.parseInt(tokens[0])); // zip
        inst.setValue(1, Integer.parseInt(tokens[1])); // hour
        inst.setValue(2, Integer.parseInt(dtkns[0])); // monthly
        List<String> lofst = new ArrayList<String>();
        HashMap<String, Double> lofsthm = new HashMap<String, Double>();
        // String st = "";
        try {
          outputs = myKDTree.kNearestNeighbours(inst, 3);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        for (int k = 0; k < outputs.numInstances(); k++) {
          output = outputs.instance(k);
          String[] streets = csvp.parseLine(output.toString());
          String streetname = streets[3].split(":")[0];
          
          if (lofsthm.get(streetname) ==null ) {
            lofsthm.put(streetname,
                Double.parseDouble(streets[3].split(":")[1].replace("'", "")));
          } else {
            Double val = lofsthm.get(streetname);
            lofsthm.put(streetname,
            Double.parseDouble(streets[3].split(":")[1].replace("'", "")) + val);
          }
        }
        
        for (Entry<String, Double> e2 : lofsthm.entrySet()) {
          lofst.add(e2.getKey().toString() + ":" + Double.toString(e2.getValue()));
        }
        Collections.sort(lofst, new WeightComparator());
        context.write(new Text(e1.getKey()), new Text(buildString(lofst)));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: kdtree_knn <in> <infuture> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "knn");
    job.setJarByClass(kdtree_knn.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(knnMapper.class);
    job.setPartitionerClass(customZipPartitioner.class);
    job.setReducerClass(knnReducer.class);
    job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
