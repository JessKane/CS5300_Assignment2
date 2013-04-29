/*
 * must change file paths of nodesTxt, edgesTxt, and blocksTxt if you want to use these methods
 */

package mapred;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Helper {
	static File nodesTxt = new File("input_files/nodes.txt");
	static File edgesTxt = new File("input_files/edges.txt");
	static File blocksTxt = new File("input_files/blocks.txt");
	static double fromNetID = 0.46;
	static double rejectMin = 0.99 * fromNetID;
	static double rejectLimit = rejectMin + 0.01;
	static ConcurrentHashMap<String,String> nodes= parseNodes(nodesTxt);
	static ArrayList<String> blocks = parseBlocks(blocksTxt);
	
	
	private static ConcurrentHashMap<String, String> parseNodes(File nodesTxt){
		System.out.println("parsing nodes");
		Scanner scanner = null;
	    ConcurrentHashMap<String,String> nodes= new ConcurrentHashMap<String, String>();
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
		        	if (outNeighbors.containsKey(dest)){
		        		outNeighbors.get(dest).add(src);
		        	}
		        	else{
		        		ArrayList<String> outNeighborList = new ArrayList<String>();
		        		outNeighborList.add(src);
		        		outNeighbors.put(dest, outNeighborList);
		        	}
		        }
		 }
		return outNeighbors;
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
	
	private static void writePRInputFile(){
		PrintWriter out = null;
		try {
			out = new PrintWriter(new FileOutputStream("/media/OS_/CS5300/cs5300_proj2/input_files/output.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 System.out.println("getting outneighbors");
		ConcurrentHashMap<String,ArrayList<String>> outNeighborsHT = getOutNeighbors();
		System.out.println("outneighbors done");
	 	for(String node: nodes.keySet()){
	 		 out.write(node+","+Double.MAX_VALUE+",");
	 		 ArrayList<String> outNeighbors = outNeighborsHT.get(node);
	 		 if (!(outNeighbors == null)){
//		 		 System.out.println(node + "'s neighbors: " + outNeighbors);
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
	}
	
	public static void print(Object nodes2){
		System.out.println(nodes2.toString());
	}
	
	public static void main(String[] args) throws Exception {
		
		print("hello");
		print(getEdgesInBlock("27").size());
	}
}
