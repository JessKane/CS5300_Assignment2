package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * MapReduce PageRank, with each node having it's own reducer.
 *
 */
public class SimplePageRank {

	static int totalNodes = 685230;
	
	//used for Hadoop Counters (to "preserve" double values)
	static double counterMultiplier = 100000000.0;

	public static enum MATCH_COUNTER {
		CONVERGENCE
	};
	
	/**
	 * The mapper. Takes in a file with info on each node, and sends the appropriate data to 
	 * each reducer. 
	 * 
	 * File format: list of (current node u, pr(u), {v|u->v})
	 * 
	 * Output: 
     * 1: a list of all edges {u, v|u->v}
     * 2: for each outgoing node, pagerank/deg(u)
	 * 3: for each node, the original PageRank value (used to calculate convergence)
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			
			String line = value.toString();

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
				

				output.collect(outputKey, outputValue);
			}

            //output #3 : for convergence - send out old page rank
			output.collect(new Text(nodeU.toString()), new Text("oldPR," + pageRankU.toString()));
		}
	}

	/**
	 * The reducer, for a single node. Takes mapper input, and writes the appropriate output file. 
	 * 
	 * Input: node v, {{w|v->w},{PR(u)/deg(u) | u->v}}
	 * Compute: PR(v) = (1-d)/N + d~(PR(u)/deg(u))
	 * Output: current node v, pr(v), {w|v->w}
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private static double d = 0.85;

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// Get the input			
			ArrayList<Integer> outlinks = new ArrayList<Integer>();
			HashMap<Integer,Double> pageRankValues = new HashMap<Integer,Double>();
			double pageRankSum = 0.0;
			Double oldPR = 0.0;
			while (values.hasNext()) {
				String line = values.next().toString();

				// The two types of input are distinguished by prefix.
				if (line.startsWith("links")) {
					String[] splitLine = line.split(",");
					for (int i = 1; i < splitLine.length; i++) { // we don't care about the prefix
						outlinks.add(Integer.parseInt(splitLine[i]));
					}
				} else if (line.startsWith("pr")) {
					String[] splitLine = line.split(",");
					
					
					pageRankValues.put(Integer.parseInt(splitLine[1]), Double.parseDouble(splitLine[2]));
					pageRankSum += Double.parseDouble(splitLine[2]);

				}
				else if (line.startsWith("oldPR")){
					String[] splitLine = line.split(",");
					oldPR = Double.parseDouble(splitLine[1]);
				}
			}
			
			// Compute New PageRank Value
			Double newPageRank = (1-d) + pageRankSum*d;
			
			//calculate node's residual
			Double convergenceValue =
					Math.abs(oldPR - newPageRank)/newPageRank;
			
			//add residual to residual-sum, implemented using Hadoop Counters
			reporter.getCounter(MATCH_COUNTER.CONVERGENCE).increment((long) (convergenceValue*counterMultiplier));
			
			// Emit the current data
			String sb = "";
			sb = newPageRank + ",";
			for (Integer edge: outlinks){
				sb+= edge + ",";
			}
			sb = sb.substring(0, sb.length()-1);
			output.collect(key, new Text(sb));
		}
	}

	/**
	 * Runs mapReduce implementation of PageRank for a number of passes
	 * @param args: args[0] = input, args[1] = output, args[2] = number of passes
	 * If user does not indicate a number of passes, defaults to one map-reduce pass
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		ArrayList<Double> convergences = new ArrayList<Double>();
		int numPasses = 1;
		if (args.length >= 3){
			numPasses = Integer.parseInt(args[2]);
		}
		for (int pass = 0; pass < numPasses; pass++){
			JobConf conf = new JobConf(SimplePageRank.class);
			conf.setJobName("simple_page_rank");
	
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
	
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
	
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
	
			//use output of pass i for input of pass i+1 if more than one pass. 
			if(pass == 0){
				FileInputFormat.setInputPaths(conf, new Path(args[0]));
			}else{
				FileInputFormat.setInputPaths(conf, new Path(args[1] + (pass - 1)));
			}
			FileOutputFormat.setOutputPath(conf, new Path(args[1] + pass));
				
			Job job = new Job(conf);
			 job.waitForCompletion(true);
			
			 //for each pass, record the average residual value into an ArrayList
			convergences.add(pass,(job.getCounters().findCounter(MATCH_COUNTER.CONVERGENCE).getValue()) / counterMultiplier / totalNodes);
		}
		
		
		//Print out average residual values for each pass
		for (int pass = 0; pass < convergences.size(); pass++){
			System.out.println("Convergence for pass " + pass + ": " + convergences.get(pass));
		}
	}
}

