/*
 * must change file paths of nodesTxt, edgesTxt, and blocksTxt if you want to use these methods
 */

package mapred;
import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Helper {
	static Properties pathProperties = new Properties();
//	static File nodesTxt = new File("/media/OS_/CS5300/cs5300/nodes1.txt");
//	static File edgesTxt = new File("/home/eric/hadoop/input_files/edges.txt");
//	static File blocksTxt = new File("/home/eric/hadoop/input_files/blocks.txt");
	
	static File nodesTxt = new File("/media/OS_/CS5300/cs5300_proj2/input_files/nodes1.txt");
	static File edgesTxt = new File("/media/OS_/CS5300/cs5300_proj2/input_files/edges1.txt");
	static File blocksTxt = new File("/media/OS_/CS5300/cs5300_proj2/input_files/blocks1.txt");

	static double fromNetID = 0.46;
	static double rejectMin = 0.99 * fromNetID;
	static double rejectLimit = rejectMin + 0.01;
	static ConcurrentHashMap<String,String> nodes= parseNodes(nodesTxt);
	static ArrayList<String> blocks = parseBlocks(blocksTxt);
	
	
	private static ConcurrentHashMap<String, String> parseNodes(File nodesTxt){
		System.out.println("parsing nodes");
		Scanner scanner = null;
	    ConcurrentHashMap<String,String> nodes= new ConcurrentHashMap<String, String>();
		//scanner = new Scanner(nodesTxt);
	    
	    pathProperties.getClass();
		try {
			scanner = new Scanner(nodesTxt);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	    while(scanner.hasNext()){
	        String line = (scanner.nextLine());
	        String result[] = line.split("\\s+");
//	        System.out.println(result[0]+ ", " +result[1]);
	        if (result[0].equals("")){
	        	nodes.put(result[1], result[2]);
	        }
	        else{
	        	nodes.put(result[0], result[1]);
	        }
	    }
	    
	    scanner.close();
		return nodes;
	}
	
	
	private static ArrayList<String> parseBlocks(File blocksTxt){
		System.out.println("parsing blocks");
	    Scanner scanner = null;
	    ArrayList<String> blocks = new ArrayList<String>(); 
		try {
			scanner = new Scanner(blocksTxt);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		blocks.add("0"); //fill in index 0
	    while(scanner.hasNext()){
	        String line = (scanner.nextLine());
	        blocks.add(line.trim());
	    }
	    
	    scanner.close();
		return blocks;
	}
	
	private static ConcurrentHashMap<String, ArrayList<String>> getOutNeighbors(){
		ConcurrentHashMap<String, ArrayList<String>> outNeighbors = new ConcurrentHashMap<String, ArrayList<String>>();
		Scanner scanner = null;
		try {
			scanner = new Scanner(edgesTxt);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 while(scanner.hasNext()){
		        String line = (scanner.nextLine());
		        String result[] = line.split("\\s+");
		        String src= "";
		        String dest ="";
		        double prob =0.0;
		        
		        if (result[0].equals("")){
		        	src = result[1].intern();
		        	dest = result[2].intern();
		        	prob = Double.parseDouble(result[3]);
		        }
		        else{
		        	src = result[0].intern();
		        	dest = result[1].intern();
		        	prob = Double.parseDouble(result[2]);
		        }
       
		        if (!(prob>= rejectMin && prob< rejectLimit)){
		        	if (outNeighbors.containsKey(src)){
		        		outNeighbors.get(src).add(dest);
		        	}
		        	else{
		        		ArrayList<String> outNeighborList = new ArrayList<String>();
		        		outNeighborList.add(dest);
		        		outNeighbors.put(src, outNeighborList);
		        	}
		        }
		 }
		return outNeighbors;
	}
	

	private static ConcurrentHashMap<String, ArrayList<String>> getInNeighbors(){
		ConcurrentHashMap<String, ArrayList<String>> inNeighbors = new ConcurrentHashMap<String, ArrayList<String>>();
		Scanner scanner = null;
		try {
			scanner = new Scanner(edgesTxt);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 while(scanner.hasNext()){
		        String line = (scanner.nextLine());
		        String result[] = line.split("\\s+");
		        String src= "";
		        String dest ="";
		        double prob =0.0;
		        
		        if (result[0].equals("")){
		        	src = result[1].intern();
		        	dest = result[2].intern();
		        	prob = Double.parseDouble(result[3]);
		        }
		        else{
		        	src = result[0].intern();
		        	dest = result[1].intern();
		        	prob = Double.parseDouble(result[2]);
		        }
       
		        if (!(prob>= rejectMin && prob< rejectLimit)){
		        	if (inNeighbors.containsKey(dest)){
		        		inNeighbors.get(dest).add(src);
		        	}
		        	else{
		        		ArrayList<String> inNeighborList = new ArrayList<String>();
		        		inNeighborList.add(src);
		        		inNeighbors.put(dest, inNeighborList);
		        	}
		        }
		 }
		return inNeighbors;
	}
	
	/**
	 * 
	 * @param blockNum 
	 * @return edges (u,v) where u is in block blockNum. Returns an ArrayList of edges, which are defined by
	 * a ConcurrentHashmaps with keys "source" and "destination"
	 */
	public static ArrayList<ConcurrentHashMap<String,String>> getEdgesInBlock(String blockNum){
		ArrayList<ConcurrentHashMap<String,String>>  BE = 
				new ArrayList<ConcurrentHashMap<String,String>>();
		Scanner scanner = null;
		try {
			scanner = new Scanner(edgesTxt);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 while(scanner.hasNext()){
		        String line = (scanner.nextLine());
		        String result[] = line.split("\\s+");
		        String src= "";
		        String dest ="";
		        
		        if (result[0].equals("")){
		        	src = result[1].intern();
		        	dest = result[2].intern();
		        }
		        else{
		        	src = result[0].intern();
		        	dest = result[1].intern();
		        }
		
			int srcNode = Integer.parseInt(src);
			int blockNumInt = Integer.parseInt(blockNum);
			int blockLowerBound = Integer.parseInt(blocks.get(Integer.parseInt(blockNum)));
			int blockUpperBound = Integer.MAX_VALUE;
			if (!(blockNumInt == blocks.size() - 1)){
				blockUpperBound = Integer.parseInt(blocks.get(Integer.parseInt(blockNum)+1));
			}
			
			if (srcNode >= blockLowerBound && srcNode < blockUpperBound){
				ConcurrentHashMap<String,String> edgeLine = new ConcurrentHashMap<String,String>();
				edgeLine.put("source", src);
				edgeLine.put("destination", dest);
				
				BE.add(edgeLine );
			}
			
		}
		return BE;
		
	}
	
	public static String getBlockId(String nodeId){
		int nodeIdInt = Integer.parseInt(nodeId);
		for(int indexOfFirst = 0; indexOfFirst < blocks.size(); indexOfFirst++){
			if(nodeIdInt < Integer.parseInt(blocks.get(indexOfFirst))){
				return (indexOfFirst - 1) + "";
			}
		}
		return -1 + "";
	}
	
	/*for EC
	 * 
	 * partitions edges into random blocks according to hash function
	 */
	public static String getBlockID_RandPart(String node){
		return "" + (hash(node)%68);
	}
	
	private static int hash(String node){
		char ch[] = node.toCharArray();
		int strLength = node.length();
		
		int i, sum;
		
		for (i = 0, sum = 0; i < strLength; i++)
			sum += ch[i];
		
		return sum;
	}
	
	/**
	 * 
	 * @param node
	 * @return in-degree of node
	 */
	public static int getInDegree(String node){
		return getInNeighbors().get(node).size();
	}
	
	/**
	 * 
	 * @param node
	 * @return out-degree of node
	 */
	public static int getOutDegree(String node){
		return getOutNeighbors().get(node).size();
	}
	
	/**
	 * 
	 * @param node
	 * @return total of in and out degree of node
	 */
	public static int getTotalDegree(String node){
		return getInDegree(node) + getOutDegree(node);
	}
	
	private static int writePRInputFile(){
		Double initial_PR = 1.0;
		PrintWriter out = null;
		int numNodes = 0;
		try {
			out = new PrintWriter(new FileOutputStream("input_files/output.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("getting outneighbors");
		ConcurrentHashMap<String,ArrayList<String>> outNeighborsHT = getOutNeighbors();
		System.out.println("outneighbors done");
		for(String node: nodes.keySet()){
			numNodes++;
			out.write(node+"\t"+initial_PR+",");
			ArrayList<String> outNeighbors = outNeighborsHT.get(node);
			if (!(outNeighbors == null)){
//				System.out.println(node + "'s neighbors: " + outNeighbors);
				for(int i = 0; i < outNeighbors.size(); i++){
					out.write(outNeighbors.get(i));
					if (i!=outNeighbors.size()-1){
						out.write(",");
					}
				}
			}
			
			out.write("\n");
			System.out.println("node " + node + " done writing");
		}
		out.close();
		System.out.println("done writing file");
		return numNodes;
	}
	
	private static int writeBlockedPRInputFile(){
		Double initial_PR = 1.0;
		PrintWriter out = null;
		int numNodes = 0;
		try {
			out = new PrintWriter(new FileOutputStream("input_files/output.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("getting inNeighbors");
		ConcurrentHashMap<String,ArrayList<String>> inNeighborsHT = getInNeighbors();
		System.out.println("inNeighbors done");
		for(String node: nodes.keySet()){
			numNodes++;
			out.write(node+"\t"+initial_PR+",");
			ArrayList<String> inNeighbors = inNeighborsHT.get(node);
			if (!(inNeighbors == null)){
//				System.out.println(node + "'s neighbors: " + outNeighbors);
				for(int i = 0; i < inNeighbors.size(); i++){
					out.write(inNeighbors.get(i));
					if (i!=inNeighbors.size()-1){
						out.write(",");
					}
				}
			}
			out.write("\n");
			System.out.println("node " + node + " done writing");
		}
		out.close();
		System.out.println("done writing file");
		return numNodes;
	}
	
	
	public static void main(String[] args) throws Exception {
		writePRInputFile();
	}
}
