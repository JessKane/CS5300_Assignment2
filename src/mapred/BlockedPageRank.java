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
	static Double convergenceBound = 0.001;

	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		// file with list of (current node u, pr(u), {v|u->v})
		// input: one line: (current node u, pr(u), {v|u->v})
		// 			int,double,int,int,(repeating int-int)

		// output: node v, {{w|v->w},{PR(u)/deg(u) | u->v}}
		// 1: a list of all edges {u, v|u->v}
		// 2: for each outgoing node, pagerank/deg(u)
		
		//Helper helper = new Helper();
	
		boolean useGaussSeidel;
		boolean useRandomBlocking;
		int numIterations = -1;
		public void configure(JobConf job){
			if(job.get("useGaussSeidel").equals("true")){
				useGaussSeidel = true;
			} else{
				useGaussSeidel = false;
			}
			
			if(job.get("useRandomBlocking").equals("true")){
				useRandomBlocking = true;
			} else{
				useRandomBlocking = false;
			}
			
			if(job.get("numIterations") != null){
				numIterations = Integer.parseInt(job.get("numIterations"));
			}
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			
			String line = value.toString();
			System.out.println("mapper input: " + line);
			String nodeId = line.split("\t")[0];
			String[] mapInput = line.split("\t")[1].split(",");
			System.out.println("BREAK1 " + useGaussSeidel + " " + numIterations);
			
			String blockId = useRandomBlocking?Helper.getBlockID_RandPart(nodeId):Helper.getBlockId(nodeId);
					
			// Collect Input
			Integer nodeU = Integer.parseInt(nodeId);
			Double pageRankU = Double.parseDouble(mapInput[0]);
			ArrayList<Integer> outgoingEdges = new ArrayList<Integer>();
			for (int i = 1; i < mapInput.length; i++) {
				outgoingEdges.add(Integer.parseInt(mapInput[i]));
			}
			//Output #1 All out-edges for this node sent to node's block
			//Contains full PR value of node
			output.collect(new Text(blockId), new Text("BE,"+line.replace("\t", ",")));
			
			
			// Output #2: All edges to new blocks sent to recieving node's block
			//Contains partial PR's of emitting node
			for (Integer edge: outgoingEdges) {
				String recvBlock = useRandomBlocking?Helper.getBlockID_RandPart(edge + ""):Helper.getBlockId(edge + "");
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
		
		
		HashMap<Integer,Double> pageRankValues;
		
		boolean useGaussSeidel;
		boolean useRandomBlocking;
		int numIterations = -1;
		public void configure(JobConf job){
			if(job.get("useGaussSeidel").equals("true")){
				useGaussSeidel = true;
			} else{
				useGaussSeidel = false;
			}
			
			if(job.get("useRandomBlocking").equals("true")){
				useRandomBlocking = true;
			} else{
				useRandomBlocking = false;
			}
			
			if(job.get("numIterations") != null){
				numIterations = Integer.parseInt(job.get("numIterations"));
			}
		}

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			System.out.println("BREAK2 " + useGaussSeidel + " " + numIterations);
			
			// Get the input
			ArrayList<Integer> outlinks = new ArrayList<Integer>();
			pageRankValues = new HashMap<Integer,Double>();
			HashMap<Integer, ArrayList<Integer>> boundaryConditions = new  HashMap<Integer, ArrayList<Integer>>();
			
			//Edges in block of Emitting to Reciving Nodes
			HashMap<Integer,ArrayList<Integer>> blockEdgesE2R = new  HashMap<Integer, ArrayList<Integer>>();
			
			//Edges in block of Reciving to Emitting Nodes
			HashMap<Integer,ArrayList<Integer>> blockEdgesR2E = new  HashMap<Integer, ArrayList<Integer>>();

			double pageRankSum = 0.0;
			while (values.hasNext()) {
				String line = values.next().toString();
				//System.out.println("reducer line: " + key + " " + line);
				
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
					
					//Emitter ---> Receiving nodes, only necessary for ordering in Gauss-Seidel
					if(blockEdgesE2R.containsKey(emitNode)){
						blockEdgesE2R.get(emitNode).addAll(recvNodes);
					}
					else{
						blockEdgesE2R.put(emitNode, recvNodes);
					}
					
					//Reverse the lookup behavior (Receiving ----> ALL Emitting nodes) to boost lookup time in block iterations
					for(Integer recvNode : recvNodes){
						if(blockEdgesR2E.containsKey(recvNode)){
							blockEdgesR2E.get(recvNode).add(emitNode);
						} else{
							ArrayList<Integer> emitHolder = new ArrayList<Integer>();
							emitHolder.add(emitNode);
							blockEdgesR2E.put(recvNode, emitHolder);
						}
					}
					
				} else if (line.startsWith("BC,")) {
					
					//Reverse the lookup behavior (Receiving ----> ALL Emitting nodes) to boost lookup time in block iterations
					for(Integer recvNode : recvNodes){
						if(boundaryConditions.containsKey(recvNode)){
							boundaryConditions.get(recvNode).add(emitNode);
						} else{
							ArrayList<Integer> emitHolder = new ArrayList<Integer>();
							emitHolder.add(emitNode);
							boundaryConditions.put(recvNode, emitHolder);
						}
					}
				}
			}
			
			
			ArrayList<Integer> orderedNodes;
			//Sort block elements according to decreasing number of outbound edges for each edge
			if(useGaussSeidel){
				orderedNodes = merge_sort(new ArrayList<Integer> (blockEdgesE2R.keySet()), blockEdgesE2R);
			} 
			//If not using GaussSeidel, order doesn't matter
			else{
				orderedNodes = new ArrayList<Integer> (blockEdgesE2R.keySet());
			}
			
			//Iterate through block
			if(numIterations > 0){
				for(int i = 0; i < numIterations; i++){
					IterateBlockOnce(orderedNodes, blockEdgesE2R, blockEdgesR2E, boundaryConditions);
				}
			} else{
				Double avgErr = Double.MAX_VALUE;
				int iterationsUsed = 0;
				while(avgErr > convergenceBound){
					avgErr = IterateBlockOnce(orderedNodes, blockEdgesE2R, blockEdgesR2E, boundaryConditions);
					iterationsUsed++;
				}
				System.out.println("ITERATIONS USED ON BLOCK" + key + ": " + iterationsUsed);
			}
			
			
			// Emit the current data
			for(Integer v : blockEdgesE2R.keySet()){
				key = new Text(v + "");
				
				String sb = "";
				sb = pageRankValues.get(v) + ",";
				for (Integer edge: blockEdgesE2R.get(v)){
					sb+= edge + ",";
				}
				sb = sb.substring(0, sb.length()-1);
				//System.out.println("reducer output: (" + key + ","+ sb+")");
				output.collect(key, new Text(sb));
			}
			
			System.out.println("^^^^^^\n");
		}
		
		public ArrayList<Integer> merge_sort(ArrayList<Integer> m, HashMap<Integer,ArrayList<Integer>> blockEdgesE2R ){
		    // if list size is 0 (empty) or 1, consider it sorted and return it
		    // (using less than or equal prevents infinite recursion for a zero length m)
		    if(m.size() <= 1)
		        return m;
		    // else list size is > 1, so split the list into two sublists
		    ArrayList<Integer> left = new ArrayList<Integer>();
		    ArrayList<Integer> right = new ArrayList<Integer>();

		    int middle = m.size() / 2;
		    
		    for(int i = 0; i < m.size(); i++){
		    	if(i < middle){
		    		left.add(m.get(i));
		    	} else{
		    		right.add(m.get(i));
		    	}
		    }
		    
		    // recursively call merge_sort() to further split each sublist
		    // until sublist size is 1
		    left = merge_sort(left, blockEdgesE2R);
		    right = merge_sort(right, blockEdgesE2R);
		    // merge the sublists returned from prior calls to merge_sort()
		    // and return the resulting merged sublist
		    return merge(left, right, blockEdgesE2R);
		}
		
		public ArrayList<Integer> merge(ArrayList<Integer> left, ArrayList<Integer> right, HashMap<Integer,ArrayList<Integer>> blockEdgesE2R ){
			ArrayList<Integer> result = new ArrayList<Integer>();

			while(left.size() > 0 || right.size() > 0){
				if(left.size() > 0 && right.size() > 0){
					if(blockEdgesE2R.get(left.get(0)).size() >= blockEdgesE2R.get(right.get(0)).size()){
						result.add(left.remove(0));
					} else{
						result.add(right.remove(0));
					}
				} else if(left.size() > 0){
					result.add(left.remove(0));
				} else if(right.size() > 0){
					result.add(right.remove(0));
				}
			}
			return result;
		}
		
		
		public Double IterateBlockOnce(ArrayList<Integer> orderedNodes, HashMap<Integer, ArrayList<Integer>> blockEdgesE2R, 
				HashMap<Integer, ArrayList<Integer>> blockEdgesR2E, HashMap<Integer, ArrayList<Integer>> boundaryConditions){
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
			
			ArrayList<Double> errorVals = new ArrayList<Double>();
			HashMap<Integer, Double> newPageRanks = new HashMap<Integer, Double>();
			for(Integer v : orderedNodes){
				newPageRanks.put(v, 0.0);
			}
			for(Integer v : orderedNodes){
				if(blockEdgesR2E.containsKey(v)){
					for(Integer u : blockEdgesR2E.get(v)){
						//Should be implicitly an edge in the block
						//if(blockEdges.keySet().contains(u)){
							newPageRanks.put(v, newPageRanks.get(v) + pageRankValues.get(u)/blockEdgesE2R.get(u).size());
						//}
					}
				}
				
				if(boundaryConditions.containsKey(v)){
					for(Integer u : boundaryConditions.get(v)){
						newPageRanks.put(v, newPageRanks.get(v) + pageRankValues.get(u));
					}
				}
				
				Double newPageRank = ((1-d)/totalNodes) + newPageRanks.get(v)*d;
				errorVals.add(Math.abs(pageRankValues.get(v) - newPageRank) / newPageRank);
				if(useGaussSeidel){
					pageRankValues.put(v, newPageRank);
				} else{
					newPageRanks.put(v, newPageRank);
				}
				
			}
			if(!useGaussSeidel){
				for(Integer v : orderedNodes){
					pageRankValues.put(v, newPageRanks.get(v));
				}
			}
			
			//Return Average residual error
			Double errSum= 0.0; 
		     for (Double i:errorVals){
		    	 errSum += i;
		     }
			return errSum / errorVals.size();
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
		
		conf.set("useGaussSeidel", "false");
		conf.set("useRandomBlocking", "false");
		
		//Check for additional parameter
		if(args.length > 2){
			if(args[2].equals("gaussSeidel")){
				conf.set("useGaussSeidel", "true");
			} else if(args[2].equals("randomBlocking")){
				conf.set("useRandomBlocking", "true");
			} else{
				try{
					conf.set("numIterations", Integer.parseInt(args[2]) + "");
				} catch(NumberFormatException e){
					System.out.println("Unknown third argument.  Please either remove, provide a numerical number of block iterations, or use 'gaussSeidel' or 'randomBlocking'.");
					return;
				}
			}
		}
		if(args.length > 3){
			try{
				conf.set("numIterations", Integer.parseInt(args[3]) + "");
			} catch(NumberFormatException e){
				System.out.println("Unknown fourth argument.  Please either remove or provide a numerical number of block iterations.");
				return;
			}
		}
		
		
		JobClient.runJob(conf);
	}
}
