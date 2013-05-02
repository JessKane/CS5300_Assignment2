//Helper.java contains several helper functions that may or may not be used in SimplePageRank and BlockedPageRank

package mapred;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Helper {
	static Properties pathProperties = new Properties();
	
	//Don't change these now, they're they're linked to the textfiles in the same location as the class file.
	static String nodesTxt = "input_files/nodes.txt";
	static String edgesTxt = "input_files/edges.txt";
	static String blocksTxt = "blocks.txt";

	
	//values used to create a subset of edges
	static double fromNetID = 0.46; //based on netID awc64
	static double rejectMin = 0.99 * fromNetID;
	static double rejectLimit = rejectMin + 0.01;
	
	//ConcurrentHashMap<String,String> nodes= parseNodes(nodesTxt);
	ConcurrentHashMap<String,String> nodes= null;
	ArrayList<String> blocks = parseBlocks(blocksTxt);
	static int totalNodes = 685230;
	
	/**
	 * parses nodes.txt into a ConcurrentHashMap of <node, block> 
	 * @param nodesTxt - nodes.txt from cms
	 * @return
	 */
	private ConcurrentHashMap<String, String> parseNodes(String nodesTxt){
		System.out.println("parsing nodes");
		InputStream is = getClass().getResourceAsStream(nodesTxt);
	    InputStreamReader isr = new InputStreamReader(is);
	    BufferedReader br = new BufferedReader(isr);
	    String line;
	    
	    ConcurrentHashMap<String,String> nodes= new ConcurrentHashMap<String, String>();
	    
	    try {
			while ((line = br.readLine()) != null) {
				String result[] = line.split("\\s+");
		        if (result[0].equals("")){
		        	nodes.put(result[1], result[2]);
		        }
		        else{
		        	nodes.put(result[0], result[1]);
		        }
			}
			br.close();
		    isr.close();
		    is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
		return nodes;
	}
	
	/**
	 * parse blocks.txt to an ArrayList
	 * @param blocksTxt - blocks.txt (from cms)
	 * @return ArrayList of values found in blocks.txt
	 */
	private ArrayList<String> parseBlocks(String blocksTxt){
		System.out.println("parsing blocks");
	    ArrayList<String> blocks = new ArrayList<String>(); 
	    
	    InputStream is = getClass().getResourceAsStream(blocksTxt);
	    InputStreamReader isr = new InputStreamReader(is);
	    BufferedReader br = new BufferedReader(isr);
	    String line;
		
		blocks.add("0"); //fill in index 0
	    try {
			while ((line = br.readLine()) != null) {
				blocks.add(line.trim());
			}
			br.close();
		    isr.close();
		    is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
		return blocks;
	}
	
	/**
	 * Reads through edges.txt, building up a list of outgoing neighbors for each node
	 * @return A ConcurrentHashMap of nodes and its out-neighbors (nodes that the key node points to)
	 */
	private ConcurrentHashMap<String, ArrayList<String>> getOutNeighbors(){
		ConcurrentHashMap<String, ArrayList<String>> outNeighbors = new ConcurrentHashMap<String, ArrayList<String>>();
		 InputStream is = getClass().getResourceAsStream(edgesTxt);
	    InputStreamReader isr = new InputStreamReader(is);
	    BufferedReader br = new BufferedReader(isr);
	    String line;
	    
	    try{
		    while ((line = br.readLine()) != null) {
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
	       
			        //add edge if it does not fall in rejection range
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
		    br.close();
		    isr.close();
		    is.close();
	    } catch(IOException e){
	    	e.printStackTrace();
	    }
		return outNeighbors;
	}
	

	/**
	 * Reads through edges.txt, building up a list of incoming neighbors for each node
	 * @return A ConcurrentHashMap of nodes and its in-neighbors (nodes that point to the key node)
	 */
	private ConcurrentHashMap<String, ArrayList<String>> getInNeighbors(){
		ConcurrentHashMap<String, ArrayList<String>> inNeighbors = new ConcurrentHashMap<String, ArrayList<String>>();
		 InputStream is = getClass().getResourceAsStream(edgesTxt);
		    InputStreamReader isr = new InputStreamReader(is);
		    BufferedReader br = new BufferedReader(isr);
		    String line;
		    
		    try{
			    while ((line = br.readLine()) != null) {
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
	       
			      //add edge if it does not fall in rejection range
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
			    br.close();
			    isr.close();
			    is.close();
		    } catch(IOException e){
		    	e.printStackTrace();
		    }
		return inNeighbors;
	}
	
	/**
	 * 
	 * @param blockNum 
	 * @return edges (u,v) where u is in block blockNum. Returns an ArrayList of edges, which are defined by
	 * a ConcurrentHashmaps with keys "source" and "destination"
	 */
	public ArrayList<ConcurrentHashMap<String,String>> getEdgesInBlock(String blockNum){
		ArrayList<ConcurrentHashMap<String,String>>  BE = 
				new ArrayList<ConcurrentHashMap<String,String>>();
		InputStream is = getClass().getResourceAsStream(edgesTxt);
	    InputStreamReader isr = new InputStreamReader(is);
	    BufferedReader br = new BufferedReader(isr);
	    String line;
	    
	    try{
		    while ((line = br.readLine()) != null) {
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
	    } catch(IOException e){
	    	e.printStackTrace();
	    }
	
	
		return BE;
		
	}
	
	/**
	 * 
	 * @param nodeId
	 * @return blockID of node identified by nodeID
	 */
	public String getBlockId(String nodeId){
		int nodeIdInt = Integer.parseInt(nodeId);
		for(int indexOfFirst = 0; indexOfFirst < blocks.size(); indexOfFirst++){
			if(nodeIdInt < Integer.parseInt(blocks.get(indexOfFirst))){
				return (indexOfFirst - 1) + "";
			}
		}
		return -1 + "";
	}
	
	/**
	 * Randomly Partitioned Block Extra Credit
	 * Randomly assigns a node to a block according to the hash function
	 * @param node - node to be assigned to a block
	 * @return the blockID to which the node was assigned 
	 */
	public static String getBlockID_RandPart(String node){
		return "" + (hash(node)%68);
	}
	
	/**
	 * Converts String node into a char array and sums up the char values for each character 
	 * @param node - node to be hashed
	 * @return
	 */
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
	public int getInDegree(String node){
		return getInNeighbors().get(node).size();
	}
	
	/**
	 * 
	 * @param node
	 * @return out-degree of node
	 */
	public int getOutDegree(String node){
		return getOutNeighbors().get(node).size();
	}
	
	/**
	 * 
	 * @param node
	 * @return total of in and out degree of node
	 */
	public int getTotalDegree(String node){
		return getInDegree(node) + getOutDegree(node);
	}
	
	/**
	 * Creates the input file used in SimplePageRank and BlockedPageRank
	 * Writes files to output.txt
	 * Format: node	initial PR value, {outgoing neighbors}
	 * @return number of nodes
	 */
	private int writePRInputFile(){
		Double initial_PR = 1.0/totalNodes;
		PrintWriter out = null;
		int numNodes = 0;
		try {
			out = new PrintWriter(new FileOutputStream("input_files/output.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//get outgoing neighbors
		ConcurrentHashMap<String,ArrayList<String>> outNeighborsHT = getOutNeighbors();
		
		//write input text file 
		for(String node: nodes.keySet()){
			numNodes++;
			out.write(node+"\t"+initial_PR+",");
			ArrayList<String> outNeighbors = outNeighborsHT.get(node);
			if (!(outNeighbors == null)){
				for(int i = 0; i < outNeighbors.size(); i++){
					out.write(outNeighbors.get(i));
					if (i!=outNeighbors.size()-1){
						out.write(",");
					}
				}
			}
			
			out.write("\n");
		}
		out.close();
		return numNodes;
	}
	
	  public void test3Columns() throws IOException{
	    InputStream is = getClass().getResourceAsStream("blocks.txt");
	    InputStreamReader isr = new InputStreamReader(is);
	    BufferedReader br = new BufferedReader(isr);
	    String line;
	    while ((line = br.readLine()) != null) 
	    {
	    	System.out.println(line);
	    }
	    br.close();
	    isr.close();
	    is.close();
	}
	
	//used to create page rank input file
	public static void main(String[] args) throws Exception {
		//Helper h = new Helper();
		//h.writePRInputFile();
	}
}
