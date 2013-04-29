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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class SimplePageRank {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		// file with list of (current node u, pr(u), {v|u->v})
		// input: one line: (current node u, pr(u), {v|u->v})
		// 			int,double,int,int,(repeating int-int)

		// output: node v, {{w|v->w},{PR(u)/deg(u) | u->v}}
		// 1: a list of all edges {u, v|u->v}
		// 2: for each outgoing node, pagerank/deg(u)
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			
			String line = value.toString();
			String[] mapInput = line.split(",");
						
			// Collect Input
			Integer nodeU = Integer.parseInt(mapInput[0]);
			Double pageRankU = Double.parseDouble(mapInput[1]);
			ArrayList<Integer> outgoingEdges = new ArrayList<Integer>();
			for (int i = 2; i < mapInput.length; i++) {
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
				Text outputValue = new Text ("pr," + nodeU + "," + pageRankU/outgoingEdges.size());
				output.collect(outputKey, outputValue);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private static double d = 0.5;

		// input: node v, {{w|v->w},{PR(u)/deg(u) | u->v}},
		// compute: PR(v) = (1-d)/N + d~(PR(u)/deg(u))
		// d is dampening
		// emit: current node v, pr(v), {w|v->w}

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// Get the input
			ArrayList<Integer> outlinks = new ArrayList<Integer>();
			HashMap<Integer,Double> pageRankValues = new HashMap<Integer,Double>();
			double pageRankSum = 0.0;
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
			}
			
			// Compute New PageRank Value
			
			Double newPageRank = ((1-d)/outlinks.size()) + pageRankSum*d;
			
			// Emit the current data
			StringBuilder sb = new StringBuilder();
			sb.append(key.toString());
			sb.append(",");
			sb.append(newPageRank.toString());
			sb.append(",");
			for (Integer edge: outlinks) {
				sb.append(edge);
				sb.append(",");
			}
			sb.deleteCharAt(sb.length() -1); // remove trailing comma

			output.collect(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SimplePageRank.class);
		conf.setJobName("simple_page_rank");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
