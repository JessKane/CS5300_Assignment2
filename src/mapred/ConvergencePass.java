package mapred;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class ConvergencePass {

	static int totalNodes = 685230;
	static double convergenceSum = 0.0;
	static double counterMultiplier = 100000000.0;

	
	public static enum MATCH_COUNTER {
		CONVERGENCE
	};
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		// file with list of (current node u, pr(u), {v|u->v})
		// input: one line: (current node u, pr(u), {v|u->v})
		// 			int,double,int,int,(repeating int-int)

		// output: node v, {{w|v->w},{PR(u)/deg(u) | u->v}}
		// 1: a list of all edges {u, v|u->v}
		// 2: for each outgoing node, pagerank/deg(u)
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			
			String line = value.toString();

//			System.out.println("mapper input: " + line);
			String nodeId = line.split("\t")[0];
			String[] mapInput = line.split("\t")[1].split(",");

			// Collect Input
			Integer nodeU = Integer.parseInt(nodeId);
			Double pageRankU = Double.parseDouble(mapInput[0]);
			ArrayList<Integer> outgoingEdges = new ArrayList<Integer>();
			for (int i = 1; i < mapInput.length; i++) {
				outgoingEdges.add(Integer.parseInt(mapInput[i]));
			}
			
			// Output #1: send all edges to reducer
			StringBuilder sb = new StringBuilder();
			sb.append("links,");
			for (Integer edge: outgoingEdges) {
				sb.append(edge + ",");			
			}
			sb.deleteCharAt(sb.length() -1); // remove trailing comma
			
//			System.out.println("mapper output: nodeU= " + nodeU + ", sb=" + sb.toString());
			output.collect(new Text(nodeU.toString()), new Text(sb.toString()));
			
			
			// Output #2: send pagerank to each outgoing node
			for (Integer edge: outgoingEdges) {
				Text outputKey = new Text ("" + edge);

				
				
				String newPR = "";
				if (outgoingEdges.size() == 0){
					newPR = "0";
				}
				else{
					newPR = ""+ pageRankU/outgoingEdges.size();
				}
				Text outputValue = new Text ("pr," + nodeU + "," + newPR);
				
//				System.out.println("to red " + outputKey + " " + outputValue);

				output.collect(outputKey, outputValue);
//				System.out.println("mapper output: nodeV: " + edge + ", outputVal: " + outputValue.toString());
			}

            //output #3 : for convergence - send out old page rank
			output.collect(new Text(nodeU.toString()), new Text("oldPR," + pageRankU.toString()));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private static double d = 0.85;

		// input: node v, {{w|v->w},{PR(u)/deg(u) | u->v}},
		// compute: PR(v) = (1-d)/N + d~(PR(u)/deg(u))
		// d is dampening
		// emit: current node v, pr(v), {w|v->w}

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// Get the input			
//			System.out.println("vvvvvv Reducer " + key.toString());
			ArrayList<Integer> outlinks = new ArrayList<Integer>();
			HashMap<Integer,Double> pageRankValues = new HashMap<Integer,Double>();
			double pageRankSum = 0.0;
			Double oldPR = 0.0;
			while (values.hasNext()) {
				String line = values.next().toString();
//				System.out.println("reducer line: " + key + " " + line);

				// The two types of input are distinguished by prefix.
				if (line.startsWith("links")) {
					String[] splitLine = line.split(",");
					for (int i = 1; i < splitLine.length; i++) { // we don't care about the prefix
						outlinks.add(Integer.parseInt(splitLine[i]));
					}
//					System.out.println("outlinks: " + outlinks);
				} else if (line.startsWith("pr")) {
					String[] splitLine = line.split(",");
					
					
					pageRankValues.put(Integer.parseInt(splitLine[1]), Double.parseDouble(splitLine[2]));
					pageRankSum += Double.parseDouble(splitLine[2]);

//					System.out.println("node: " + splitLine[1] + ", PR= " + splitLine[2] + ", PRSum= " + pageRankSum);
				}
				else if (line.startsWith("oldPR")){
					String[] splitLine = line.split(",");
					oldPR = Double.parseDouble(splitLine[1]);
				}
			}
			
			// Compute New PageRank Value

			Double newPageRank = (1-d) + pageRankSum*d;
			
			//print out convergence
			Double convergenceValue =
					Math.abs(oldPR - newPageRank)/newPageRank;
//			System.out.println("Convergence for node "+key+ "= " + convergenceValue);
//			System.out.println("oldPR: " + oldPR + ", NewPR: " + newPageRank);
//			System.out.println("----");	
			
			if((long)(convergenceValue*counterMultiplier)< 0){
				System.out.println("-----------LONG convergence value <" + 
						(long)(convergenceValue*counterMultiplier) + "> is negative.-----------");
			}
			convergenceSum += convergenceValue;
			reporter.getCounter(MATCH_COUNTER.CONVERGENCE).increment((long) (convergenceValue*counterMultiplier));
			
			// Emit the current data
			String sb = "";
			sb = newPageRank + ",";
			for (Integer edge: outlinks){
				sb+= edge + ",";
			}
			sb = sb.substring(0, sb.length()-1);
//			System.out.println("reducer output: (" + key + ","+ sb+")");
			output.collect(key, new Text(sb));
//			System.out.println("^^^^^^\n");
		}
	}

	
	public static void main(String[] args) throws Exception{
		ArrayList<Double> convergences = new ArrayList<Double>();
		for (int pass = 0; pass < 5; pass++){
			JobConf conf = new JobConf(SimplePageRank.class);
			conf.setJobName("simple_page_rank");
	
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
	
			conf.setMapperClass(Map.class);
	//		conf.setCombinerClass(Reduce.class);
			conf.setReducerClass(Reduce.class);
	
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
	
			if(pass == 0){
				FileInputFormat.setInputPaths(conf, new Path(args[0]));
			}else{
				FileInputFormat.setInputPaths(conf, new Path(args[1] + (pass - 1)));
			}
			FileOutputFormat.setOutputPath(conf, new Path(args[1] + pass));
			
	//		JobClient.runJob(conf);
	
			Job job = new Job(conf);
			 job.waitForCompletion(true);
			
			convergences.add(pass,(job.getCounters().findCounter(MATCH_COUNTER.CONVERGENCE).getValue()) / counterMultiplier / totalNodes);
		}
		
		for (int pass = 0; pass < convergences.size(); pass++){
			System.out.println("Convergence for pass " + pass + ": " + convergences.get(pass));
		}
	}
}

