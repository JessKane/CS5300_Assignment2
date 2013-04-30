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

public class BlockedPageRank {

	static int totalNodes = 685230;
	
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		// file with list of (current node u, pr(u), {v|u->v})
		// input: one line: (current node u, pr(u), {v|u->v})
		// 			int,double,int,int,(repeating int-int)

		// output: node v, {{w|v->w},{PR(u)/deg(u) | u->v}}
		// 1: a list of all edges {u, v|u->v}
		// 2: for each outgoing node, pagerank/deg(u)
		
		//Helper helper = new Helper();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			
			String line = value.toString();
			
			System.out.println("mapper input: " + line);
			String nodeId = line.split("\t")[0];
			String[] mapInput = line.split("\t")[1].split(",");
			String blockId = Helper.getBlockId(nodeId);
					
			// Collect Input
			Integer nodeU = Integer.parseInt(nodeId);
			Double pageRankU = Double.parseDouble(mapInput[0]);
			ArrayList<Integer> outgoingEdges = new ArrayList<Integer>();
			for (int i = 1; i < mapInput.length; i++) {
				//NOT SURE IF THIS IS NECESSARY
				//But i don't think we need edges going OUT of the block
				
				
				outgoingEdges.add(Integer.parseInt(mapInput[i]));
				
				//Output #1 Edges within (or directed out of of) the block
				//Contains full PR value of node
			}
			output.collect(new Text(blockId), new Text("BE,"+line.replace("\t", ",")));
			
			
			// Output #2: send partial pagerank along edges entering different blocks
			for (Integer edge: outgoingEdges) {
				String recvBlock = Helper.getBlockId(edge + "");
				if(!recvBlock.equals(blockId)){
					Text outputKey = new Text (recvBlock);

					String newPR = "";
					if (outgoingEdges.size() == 0){
						newPR = "0";
					}
					else{
						newPR = ""+ pageRankU/outgoingEdges.size();
					}
					Text outputValue = new Text ("BC," + nodeU + "," + newPR + "," + edge);
					
					System.out.println("to red " + outputKey + " " + outputValue);

					output.collect(outputKey, outputValue);
//					System.out.println("mapper output: nodeV: " + edge + ", outputVal: " + outputValue.toString());
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private static double d = 0.85;

		// input: node v, {{w|v->w},{PR(u)/deg(u) | u->v}},
		// compute: PR(v) = (1-d)/N + d~(PR(u)/deg(u))
		// d is dampening
		// emit: current node v, pr(v), {w|v->w}
		
		int numIterations = 2;

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// Get the input
			ArrayList<Integer> outlinks = new ArrayList<Integer>();
			HashMap<Integer,Double> pageRankValues = new HashMap<Integer,Double>();
			HashMap<Integer, ArrayList<Integer>> boundaryConditions = new  HashMap<Integer, ArrayList<Integer>>();
			HashMap<Integer,ArrayList<Integer>> blockEdges = new  HashMap<Integer, ArrayList<Integer>>();

			double pageRankSum = 0.0;
			while (values.hasNext()) {
				String line = values.next().toString();
				System.out.println("reducer line: " + key + " " + line);
				
				String[] splitLine = line.split(",");
				Integer emitNode = Integer.parseInt(splitLine[1]);
				Double pageRank = Double.parseDouble(splitLine[2]);
				ArrayList<Integer> recvNodes = new ArrayList<Integer>();
				for (int i = 3; i < splitLine.length; i++) { 
					recvNodes.add(Integer.parseInt(splitLine[i]));
				}
				
				pageRankValues.put(emitNode, pageRank);
				
				// The two types of input are distinguished by prefix.
				if (line.startsWith("BE,")) {	
					if(blockEdges.containsKey(emitNode)){
						blockEdges.get(emitNode).addAll(recvNodes);
					}
					else{
						blockEdges.put(emitNode, recvNodes);
					}
				} else if (line.startsWith("BC,")) {
					if(boundaryConditions.containsKey(emitNode)){
						boundaryConditions.get(emitNode).addAll(recvNodes);
					}
					else{
						boundaryConditions.put(emitNode, recvNodes);
					}
				}
			}
			
			for(int i = 0; i < numIterations; i++){
				
				/*for( v ∈ B ) { NPR[v] = 0; }
			    for( v ∈ B ) {
			        for( u where <u, v> ∈ BE ) {
			            NPR[v] += PR[u] / deg(u);
			        }
			        for( u, R where <u,v,R> ∈ BC ) {
			            NPR[v] += R;
			        }
			        NPR[v] = d*NPR[v] + (1-d)/N;
			    }
			    for( v ∈ B ) { PR[v] = NPR[v]; }*/
				
				HashMap<Integer, Double> newPageRanks = new HashMap<Integer, Double>();
				for(Integer v : blockEdges.keySet()){
					newPageRanks.put(v, 0.0);
				}
				for(Integer u : blockEdges.keySet()){
					for(Integer v : blockEdges.get(u)){
						//Check v is in block
						if(blockEdges.keySet().contains(v)){
							newPageRanks.put(v, pageRankValues.get(u)/blockEdges.get(u).size());
						}
					}
				}
				for(Integer u : boundaryConditions.keySet()){
					for(Integer v : boundaryConditions.get(u)){
						newPageRanks.put(v, newPageRanks.get(v) + pageRankValues.get(u));
					}
				}
				for(Integer v : blockEdges.keySet()){
					pageRankValues.put(v, ((1-d)/totalNodes) + newPageRanks.get(v)*d);
				}
				
				/*for(Integer v : blockEdges.keySet()){
					for(Integer u : blockEdges.keySet()){
						//Check that <u,v> exists
						if(blockEdges.get(u).contains(v)){
							newPageRanks.put(v, pageRankValues.get(u)/blockEdges.get(u).size());
						}
					}
					for(Integer u : boundaryConditions.keySet()){
						if(boundaryConditions.get(u).contains(v)){
							newPageRanks.put(v, newPageRanks.get(v) + pageRankValues.get(u));
						}
					}
					newPageRanks.put(v, ((1-d)/totalNodes) + newPageRanks.get(v)*d);
				}*/
				/*for(Integer v : blockEdges.keySet()){
					pageRankValues.put(v, newPageRanks.get(v));
				}*/
			}
			
			// Emit the current data
			for(Integer v : blockEdges.keySet()){
				key = new Text(v + "");
				
				String sb = "";
				sb = pageRankValues.get(v) + ",";
				for (Integer edge: blockEdges.get(v)){
					sb+= edge + ",";
				}
				sb = sb.substring(0, sb.length()-1);
				System.out.println("reducer output: (" + key + ","+ sb+")");
				output.collect(key, new Text(sb));
			}
			
			System.out.println("^^^^^^\n");
		}
	}

	public static void main(String[] args) throws Exception {
		
		JobConf conf = new JobConf(BlockedPageRank.class);
		conf.setJobName("blocked_page_rank");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
