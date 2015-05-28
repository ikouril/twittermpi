package cz.vutbr.fit.entrypoint;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

import cz.vutbr.fit.process.Filter;
import cz.vutbr.fit.process.Gender;
import cz.vutbr.fit.process.Index;
import cz.vutbr.fit.process.Lemma;
import cz.vutbr.fit.process.Ner;
import cz.vutbr.fit.process.POSTagger;
import cz.vutbr.fit.process.Parser;
import cz.vutbr.fit.process.SentenceSplitter;
import cz.vutbr.fit.process.Sentiment;
import cz.vutbr.fit.process.Tokenizer;
import cz.vutbr.fit.source.Dump;
import cz.vutbr.fit.util.Output;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import eu.juniper.sa.monitoring.agent.MonitoringAgentForSqlFile;
import eu.juniper.sa.monitoring.agent.MonitoringAgentInterface;
import eu.juniper.sa.monitoring.sensor.DataConnectionSensorInterface;
import eu.juniper.sa.monitoring.sensor.ProgramInstanceSensorInterface;
import mpi.* ;

public class EntryPoint {
	
	private static MonitoringAgentInterface monitoringAgent;

	/**
	 * Checks whether given rank is contained in an array
	 * @param rank
	 * @param array
	 * @return true, when rank is contained
	 */
	static public boolean contains(int rank,int[] array){
		if (rank >= array[0] && rank<=array[array.length-1])
			return true;
		return false;
	}
	
	/**
	 * Serializes object to byte array
	 * @param o - object to be serialized
	 * @return array of bytes
	 */
	static public byte[] obj2array(Object o){
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
			out = new ObjectOutputStream(bos);
			out.writeObject(o);
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
        return bos.toByteArray();
	}
	
	/**
	 * Deserializes array of bytes to object
	 * @param bytes - bytes to be deserialized
	 * @return deserialized object
	 */
	static public Object array2obj(byte[] bytes){
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput inStream = null;
        Object obj=null;
        try
        {
            inStream = new ObjectInputStream(bis);
            obj = inStream.readObject();
            
        }
        catch(IOException ex){
        	ex.printStackTrace();
        }
        catch(ClassNotFoundException cnf){
        	cnf.printStackTrace();
        }
        return obj;
	}
	
	/**
	 * Converts bytes to integer
	 * @param bytes - bytes to be converted
	 * @return int
	 */
	public static int toInt(byte[] bytes, int offset) {
		  int ret = 0;
		  for (int i=0; i<4 && i+offset<bytes.length; i++) {
		    ret <<= 8;
		    ret |= (int)bytes[i] & 0xFF;
		  }
		  return ret;
	}
	
	/**
	 * Converts bytes to integers
	 * @param bytes - bytes to be converted
	 * @return int array
	 */
	static public int[] bytesToInt(byte[] bytes){
		return new int[]{toInt(bytes,0),toInt(bytes,1)};
	}
	
	/**
	 * Converts integers to bytes
	 * @param message int array to be converted
	 * @return byte array
	 */
	public static final byte[] intsToByteArray(int []message) {
	    return new byte[] {
	            (byte)(message[0] >>> 24),
	            (byte)(message[0] >>> 16),
	            (byte)(message[0] >>> 8),
	            (byte)message[0],
	            (byte)(message[1] >>> 24),
	            (byte)(message[1] >>> 16),
	            (byte)(message[1] >>> 8),
	            (byte)message[1]};
	}
	
	/**
	 * Processes tweetsdata with MPI framework
	 * @param args
	 * @throws MPIException
	 * @throws JSAPException 
	 * @throws IOException 
	 */
	static public void main(String[] args) throws MPIException, JSAPException, IOException {
       	MPI.Init(args) ;  

		Dump dump=null;
		Filter filter=null;
		Gender gender=null;
		Index index=null;
		Lemma lemma=null;
		Ner ner=null;
		Parser parser=null;
		POSTagger pos=null;
		SentenceSplitter splitter=null;
		Sentiment sentiment=null;
		Tokenizer tokenizer=null;
		
		int myrank = MPI.COMM_WORLD.getRank() ;  
		
		SimpleJSAP jsap = new SimpleJSAP( EntryPoint.class.getName(), "Processes stream of tweets data.",
				new Parameter[] {
					new FlaggedOption( "id", JSAP.STRING_PARSER, "071b0a14-1e05-431b-a1d4-afa982666a91", JSAP.NOT_REQUIRED, 'i', "id", "Id of actual deployment." ),
					new FlaggedOption( "dump", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 'd', "dump", "Number of dump instances." ),
					new FlaggedOption( "filter", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 'f', "filter", "Number of filter instances." ),
					new FlaggedOption( "gender", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 'g', "gender", "Number of gender instances." ),
					new FlaggedOption( "index", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 'x', "index", "Number of index instances." ),
					new FlaggedOption( "lemma", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 'l', "lemma", "Number of lemma instances." ),
					new FlaggedOption( "ner", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 'n', "ner", "Number of ner instances." ),
					new FlaggedOption( "parser", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 'p', "parser", "Number of parser instances." ),
					new FlaggedOption( "pos", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 'o', "pos", "Number of pos instances." ),
					new FlaggedOption( "splitter", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 's', "splitter", "Number of splitter instances." ),
					new FlaggedOption( "sentiment", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 'e', "sentiment", "Number of sentiment instances." ),
					new FlaggedOption( "tokenizer", JSAP.INTEGER_PARSER, "4", JSAP.NOT_REQUIRED, 't', "tokenizer", "Number of tokenizer instances." ),
					new FlaggedOption( "granularity", JSAP.INTEGER_PARSER, "10", JSAP.NOT_REQUIRED, 'r', "granularity", "Number of records per one execution." )
				}
		);
		String hostname=null;
		try{
			hostname=InetAddress.getLocalHost().getHostName();
		}
		catch(UnknownHostException e){
			hostname="-unknown-";
		}
		

		JSAPResult jsapResult = jsap.parse( args );
		
		String id = jsapResult.getString("id");
		int dumps = jsapResult.getInt("dump");
		int filters = jsapResult.getInt("filter");
		int genders = jsapResult.getInt("gender");
		int indexes = jsapResult.getInt("index");
		int lemmas = jsapResult.getInt("lemma");
		int ners = jsapResult.getInt("ner");
		int parsers = jsapResult.getInt("parser");
		int poses = jsapResult.getInt("pos");
		int splitters = jsapResult.getInt("splitter");
		int sentiments = jsapResult.getInt("sentiment");
		int tokenizers = jsapResult.getInt("tokenizer");
		int granularity = jsapResult.getInt("granularity");
		
		//System.out.println(hostname+" "+myrank);
		
		int waitingIndexes=filters+ners+lemmas+genders+sentiments;
		int waitingFilters=dumps;
		int waitingTokenizers=filters;
		int waitingSplitters=tokenizers;
		int waitingPoses=splitters;
		int waitingGenders=poses;
		int waitingLemmas=poses;
		int waitingNers=poses;
		int waitingParsers=poses;
		int waitingSentiments=parsers;
		
		int[] dumpNodes=new int[dumps];
		int[] filterNodes=new int[filters];
		int[] genderNodes=new int[genders];
		int[] indexNodes=new int[indexes];
		int[] lemmaNodes=new int[lemmas];
		int[] nerNodes=new int[ners];
		int[] parserNodes=new int[parsers];
		int[] posNodes=new int[poses];
		int[] splitterNodes=new int[splitters];
		int[] sentimentNodes=new int[sentiments];
		int[] tokenizerNodes=new int[tokenizers];
		int i;
		for (i=0;i<dumps;i++)
			dumpNodes[i]=i;
		for (int j=0;j<filters;j++,i++)
			filterNodes[j]=i;
		for (int j=0;j<genders;j++,i++)
			genderNodes[j]=i;
		for (int j=0;j<indexes;j++,i++)
			indexNodes[j]=i;
		for (int j=0;j<lemmas;j++,i++)
			lemmaNodes[j]=i;
		for (int j=0;j<ners;j++,i++)
			nerNodes[j]=i;
		for (int j=0;j<parsers;j++,i++)
			parserNodes[j]=i;
		for (int j=0;j<poses;j++,i++)
			posNodes[j]=i;
		for (int j=0;j<splitters;j++,i++)
			splitterNodes[j]=i;
		for (int j=0;j<sentiments;j++,i++)
			sentimentNodes[j]=i;
		for (int j=0;j<tokenizers;j++,i++)
			tokenizerNodes[j]=i;
		
		Random rn = new Random();
		
		XXHashFactory factory = XXHashFactory.fastestInstance();
		XXHash32 hasher=factory.hash32();
		int seed = 0x9747b28c;
		
		Properties props=new Properties();
		props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment, gender, ner");
		StanfordCoreNLP pipeline=new StanfordCoreNLP(props);
		
		//initialize processing objects
		if (contains(myrank,dumpNodes)){
	        dump=new Dump(granularity,myrank);
	        //System.out.println("Dump: "+myrank);
		}
		else if (contains(myrank,filterNodes)){
			filter=new Filter();
			//System.out.println("Filter: "+myrank);
		}
		else if (contains(myrank,genderNodes)){
			gender=new Gender(pipeline.getExistingAnnotator("gender"));
			//System.out.println("Gender: "+myrank);
		}
		else if (contains(myrank,indexNodes)){
			index=new Index();
			//System.out.println("Index: "+myrank);
		}
		else if (contains(myrank,lemmaNodes)){
			lemma=new Lemma(pipeline.getExistingAnnotator("lemma"));
			//System.out.println("Lemma: "+myrank);
		}
		else if (contains(myrank,nerNodes)){
			ner=new Ner(pipeline.getExistingAnnotator("ner"));
			//System.out.println("Ner: "+myrank);
		}
		else if (contains(myrank,parserNodes)){
			parser=new Parser(pipeline.getExistingAnnotator("parse"));
			//System.out.println("Parser: "+myrank);
		}
		else if (contains(myrank,posNodes)){
			pos=new POSTagger(pipeline.getExistingAnnotator("pos"));
			//System.out.println("POS: "+myrank);
		}
		else if (contains(myrank,splitterNodes)){
			splitter=new SentenceSplitter(pipeline.getExistingAnnotator("ssplit"));
			//System.out.println("Splitter: "+myrank);
		}
		else if (contains(myrank,sentimentNodes)){
			sentiment=new Sentiment(pipeline.getExistingAnnotator("sentiment"));
			//System.out.println("Sentiment: "+myrank);
		}
		else if (contains(myrank,tokenizerNodes)){
			tokenizer=new Tokenizer(pipeline.getExistingAnnotator("tokenize"));
			//System.out.println("Tokenizer: "+myrank);
		}
		
            
        int aggregatedMessage[]=new int[2];
        byte bytesMessage[]=new byte[8];
        
        monitoringAgent = new MonitoringAgentForSqlFile("/ram/"+hostname+"."+granularity, id);
        ProgramInstanceSensorInterface programInstanceSensor = monitoringAgent.createProgramInstanceSensor(myrank);
        DataConnectionSensorInterface toFilter = monitoringAgent.createDataConnectionSensor(myrank, "toFilter");
        DataConnectionSensorInterface toGender = monitoringAgent.createDataConnectionSensor(myrank, "toGender");
        DataConnectionSensorInterface toIndex = monitoringAgent.createDataConnectionSensor(myrank, "toIndex");
        DataConnectionSensorInterface toLemma = monitoringAgent.createDataConnectionSensor(myrank, "toLemma");
        DataConnectionSensorInterface toNer = monitoringAgent.createDataConnectionSensor(myrank, "toNer");
        DataConnectionSensorInterface toParser = monitoringAgent.createDataConnectionSensor(myrank, "toParser");
        DataConnectionSensorInterface toPos = monitoringAgent.createDataConnectionSensor(myrank, "toPos");
        DataConnectionSensorInterface toSplit = monitoringAgent.createDataConnectionSensor(myrank, "toSplit");
        DataConnectionSensorInterface toSentiment = monitoringAgent.createDataConnectionSensor(myrank, "toSentiment");
        DataConnectionSensorInterface toTokenizer = monitoringAgent.createDataConnectionSensor(myrank, "toTokenizer");
        
        long monitor=1000/granularity;
        long counter=0;
        
        Request dumpControl=null;
        Request dumpData=null;
        Request filterControl1=null;
        Request filterData1=null;
        Request filterControl2=null;
        Request filterData2=null;
        Request tokenizerControl=null;
        Request tokenizerData=null;
        Request splitterControl=null;
        Request splitterData=null;
        Request posControl1=null;
        Request posData1=null;
        Request posControl2=null;
        Request posData2=null;
        Request posControl3=null;
        Request posData3=null;
        Request posControl4=null;
        Request posData4=null;
        Request genderControl=null;
        Request genderData=null;
        Request lemmaControl=null;
        Request lemmaData=null;
        Request nerControl=null;
        Request nerData=null;
        Request parserControl=null;
        Request parserData=null;
        Request sentimentControl=null;
        Request sentimentData=null;
        
        while (true){
        	//boolean monitorThisTime=counter%monitor==0;
        	boolean monitorThisTime=true;
        	////System.out.println("Counter: "+counter+", rank:"+myrank);
        	if (contains(myrank,dumpNodes)){
        		Output out=dump.nextData();
        		if (out!=null){
	        		byte[] bytes=obj2array(out);
	                aggregatedMessage[0]=bytes.length;
	                aggregatedMessage[1]=myrank;
	        		int to=rn.nextInt(filters)+filterNodes[0];
	        		//System.out.println("Dump sending data");
	        		ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
	        		controlBuffer.put(intsToByteArray(aggregatedMessage));
	        		ByteBuffer dataBuffer=ByteBuffer.allocateDirect(bytes.length);
	        		dataBuffer.put(bytes);
	        		
	        		if (dumpControl!=null)
	        			dumpControl.waitFor();
	        		dumpControl=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, to, 0);
	        		if (dumpData!=null)
	        			dumpData.waitFor();
	        		dumpData=MPI.COMM_WORLD.iSend(dataBuffer, bytes.length, MPI.BYTE, to, 1);

        		}
        		else{
        			//System.out.println("Dump finished");
        			aggregatedMessage[0]=-1;
        			aggregatedMessage[1]=myrank;
        			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
					controlBuffer.put(intsToByteArray(aggregatedMessage));
        			if (dumpControl!=null)
	        			dumpControl.waitFor();
        			for (int j=0;j<filterNodes.length;j++)
        				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, filterNodes[j], 0);
        			break;
        		}
    		}
    		else if (contains(myrank,filterNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toFilter.receiveStarts();
    			//System.out.println("Before receive info filter "+myrank);
    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 0);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info filter "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toFilter.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingFilters--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingFilters==0){
    					aggregatedMessage[1]=myrank;
    					if (filterControl1!=null)
    						filterControl1.waitFor();
    					controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
    					for (int j=0;j<tokenizerNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, tokenizerNodes[j], 2);
    					if (filterControl2!=null)
    						filterControl2.waitFor();
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, indexNodes[j], 18);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toFilter.receiveStarts();
    				//System.out.println("Before receive data filter "+myrank);
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 1);
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data filter "+myrank);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	        			programInstanceSensor.subtract(toFilter.receiveEnds());
	        		Output out=filter.execute(in);
	        		if (monitorThisTime)
	    				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                aggregatedMessage[0]=sendBytes.length;
		                aggregatedMessage[1]=myrank;
		                int to=rn.nextInt(tokenizers)+tokenizerNodes[0];
		                
		                controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
		                dataBuffer=ByteBuffer.allocateDirect(sendBytes.length);
		                dataBuffer.put(sendBytes);

		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            int newTo=hash%indexes;
        	            if (newTo<0)
        	            	newTo+=indexes;
        	            newTo+=indexNodes[0];
        	       
        	            filterControl1=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, to, 2);
        	            filterControl2=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, newTo, 18);
        	            
        	            boolean firstOK=false;
        	            boolean secondOK=false;
        	            
        	            while (!firstOK || !secondOK){
        	            	if (!firstOK){
        	            		if (filterControl1.test() && (filterData1==null || filterData1.test())){
        	            			firstOK=true;
        	            			filterData1=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, to, 3);
        	            		}
        	            	}
        	            	if (!secondOK){
        	            		if (filterControl2.test() && (filterData2==null || filterData2.test())){
    	            				secondOK=true;
    	            				filterData2=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, newTo, 19);
        	            		}
        	            	}
        	            }
	        		}
    			}
    		}
    		else if (contains(myrank,genderNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toGender.receiveStarts();
    			//System.out.println("Before receive info gender "+myrank);
    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 8);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info gender "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toGender.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingGenders--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingGenders==0){
    					aggregatedMessage[1]=myrank;
    					if (genderControl!=null)
    						genderControl.waitFor();
    					controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, indexNodes[j], 18);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toGender.receiveStarts();
    				//System.out.println("Before receive data gender "+myrank);
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 9);
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data gender "+myrank);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toGender.receiveEnds());
	        		Output out=gender.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                aggregatedMessage[0]=sendBytes.length;
		                aggregatedMessage[1]=myrank;

		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            int to=hash%indexes;
        	            if (to<0)
        	            	to+=indexes;
        	            to+=indexNodes[0];
        	            controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
        	            dataBuffer=ByteBuffer.allocateDirect(sendBytes.length);
        	            dataBuffer.put(sendBytes);
        	            if (genderControl!=null)
        	            	genderControl.waitFor();
        	            genderControl=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, to, 18);
        	            if (genderData!=null)
        	            	genderData.waitFor();
		        		genderData=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, to, 19);
	        		}
    			}
    		}
    		else if (contains(myrank,indexNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toIndex.receiveStarts();

    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			//System.out.println("Before receive info index "+myrank);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 18);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info index "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toIndex.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingIndexes--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingIndexes==0){
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toIndex.receiveStarts();
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
    				//System.out.println("Before receive data index "+myrank);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 19);
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data index "+myrank);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toIndex.receiveEnds());
	        		index.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
    			}
    		}
    		else if (contains(myrank,lemmaNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toLemma.receiveStarts();
    			//System.out.println("Before receive info lemma "+myrank);
    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 10);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info lemma "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toLemma.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingLemmas--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingLemmas==0){
    					aggregatedMessage[1]=myrank;
    					if (lemmaControl!=null)
    						lemmaControl.waitFor();
    					controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, indexNodes[j], 18);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toLemma.receiveStarts();
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
    				//System.out.println("Before receive data lemma "+myrank);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 11);
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data lemma "+myrank);
	        		
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toLemma.receiveEnds());
	        		Output out=lemma.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                aggregatedMessage[0]=sendBytes.length;
		                aggregatedMessage[1]=myrank;

		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            int to=hash%indexes;
        	            if (to<0)
        	            	to+=indexes;
        	            to+=indexNodes[0];
        	            
        	            controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
        	            dataBuffer=ByteBuffer.allocateDirect(sendBytes.length);
        	            dataBuffer.put(sendBytes);
        	            if (lemmaControl!=null)
        	            	lemmaControl.waitFor();
        	            lemmaControl=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, to, 18);
        	            if (lemmaData!=null)
        	            	lemmaData.waitFor();
		        		lemmaData=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, to, 19);
	        		}
    			}

    		}
    		else if (contains(myrank,nerNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toNer.receiveStarts();
    			
    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			//System.out.println("Before receive info ner "+myrank);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 12);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info ner "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toNer.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingNers--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingNers==0){
    					aggregatedMessage[1]=myrank;
    					if (nerControl!=null)
    						nerControl.waitFor();
    					controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, indexNodes[j], 18);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toNer.receiveStarts();
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
    				//System.out.println("Before receive data ner "+myrank);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 13);
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data ner "+myrank);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toNer.receiveEnds());
	        		Output out=ner.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                aggregatedMessage[0]=sendBytes.length;
		                aggregatedMessage[1]=myrank;

		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            int to=hash%indexes;
        	            if (to<0)
        	            	to+=indexes;
        	            to+=indexNodes[0];
        	            
        	            controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
        	            dataBuffer=ByteBuffer.wrap(sendBytes);
        	            if (nerControl!=null)
        	            	nerControl.waitFor();
        	            nerControl=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, to, 18);
        	            if (nerData!=null)
        	            	nerData.waitFor();
		        		nerData=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, to, 19);
	        		}
    			}
    		}
    		else if (contains(myrank,parserNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toParser.receiveStarts();
    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			//System.out.println("Before receive info parser "+myrank);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 14);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info parser "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toParser.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingParsers--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingParsers==0){
    					aggregatedMessage[1]=myrank;
    					if (parserControl!=null)
    						parserControl.waitFor();
    					controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
    					for (int j=0;j<sentimentNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, sentimentNodes[j], 16);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toParser.receiveStarts();
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
    				//System.out.println("Before receive data parser "+myrank);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 15);
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data parser "+myrank);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toParser.receiveEnds());
	        		Output out=parser.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                aggregatedMessage[0]=sendBytes.length;
		                aggregatedMessage[1]=myrank;

		        		int to = rn.nextInt(sentiments)+sentimentNodes[0];
		        		
		        		controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
		        		dataBuffer=ByteBuffer.allocateDirect(sendBytes.length);
		        		dataBuffer.put(sendBytes);
		        		
		        		if (parserControl!=null)
		        			parserControl.waitFor();
        	            parserControl=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, to, 16);
        	            if (parserData!=null)
        	            	parserData.waitFor();
		        		parserData=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, to, 17);
	        		}
    			}

    		}
    		else if (contains(myrank,posNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toPos.receiveStarts();
    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			//System.out.println("Before receive info pos "+myrank);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 6);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info pos "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toPos.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingPoses--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingPoses==0){
    					aggregatedMessage[1]=myrank;
    					controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
    					if (posControl1!=null)
    						posControl1.waitFor();
    					for (int j=0;j<genderNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, genderNodes[j], 8);
    					if (posControl2!=null)
    						posControl2.waitFor();
    					for (int j=0;j<lemmaNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, lemmaNodes[j], 10);
    					if (posControl3!=null)
    						posControl3.waitFor();
    					for (int j=0;j<nerNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, nerNodes[j], 12);
    					if (posControl4!=null)
    						posControl4.waitFor();
    					for (int j=0;j<parserNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, parserNodes[j], 14);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toPos.receiveStarts();
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
    				//System.out.println("Before receive data pos "+myrank);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 7);
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data pos "+myrank);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toPos.receiveEnds());
	        		Output out=pos.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                aggregatedMessage[0]=sendBytes.length;
		                aggregatedMessage[1]=myrank;
		                int genderTo=rn.nextInt(genders)+genderNodes[0];
		                int lemmaTo=rn.nextInt(lemmas)+lemmaNodes[0];
		                int nerTo=rn.nextInt(ners)+nerNodes[0];
		                int parserTo=rn.nextInt(parsers)+parserNodes[0];
		                controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
		                dataBuffer=ByteBuffer.allocateDirect(sendBytes.length);
		                dataBuffer.put(sendBytes);
		                
		                posControl1=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, genderTo, 8);
		                posControl2=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, lemmaTo, 10);
		                posControl3=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, nerTo, 12);
		                posControl4=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, parserTo, 14);
        	            
        	            boolean firstOK=false;
        	            boolean secondOK=false;
        	            boolean thirdOK=false;
        	            boolean fourthOK=false;
        	            
        	            while (!firstOK || !secondOK || !thirdOK || !fourthOK){
        	            	if (!firstOK){
        	            		if (posControl1.test() && (posData1==null || posData1.test())){
        	            			firstOK=true;
        	            			posData1=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, genderTo, 9);
        	            		}
        	            	}
        	            	if (!secondOK){
        	            		if (posControl2.test() && (posData2==null || posData2.test())){
        	            			secondOK=true;
        	            			posData2=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, lemmaTo, 11);
        	            		}
        	            	}
        	            	if (!thirdOK){
        	            		if (posControl3.test() && (posData3==null || posData3.test())){
        	            			thirdOK=true;
        	            			posData3=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, nerTo, 13);
        	            		}
        	            	}
        	            	if (!fourthOK){
        	            		if (posControl4.test() && (posData4==null || posData4.test())){
        	            			fourthOK=true;
        	            			posData4=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, parserTo, 15);
        	            		}
        	            	}
        	            	
        	            }
	        		}
    			}
    		}
    		else if (contains(myrank,splitterNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toSplit.receiveStarts();
    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			//System.out.println("Before receive info splitter "+myrank);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 4);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info splitter "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toSplit.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingSplitters--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingSplitters==0){
    					aggregatedMessage[1]=myrank;
    					controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
    					if (splitterControl!=null)
    						splitterControl.waitFor();
    					for (int j=0;j<posNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, posNodes[j], 6);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toSplit.receiveStarts();
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
    				//System.out.println("Before receive data splitter "+myrank);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 5);
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data splitter "+myrank);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toSplit.receiveEnds());
	        		Output out=splitter.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                aggregatedMessage[0]=sendBytes.length;
		                aggregatedMessage[1]=myrank;
		                int to=rn.nextInt(poses)+posNodes[0];
		                
		                controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
		                dataBuffer=ByteBuffer.allocateDirect(sendBytes.length);
		                dataBuffer.put(sendBytes);
		                
		                if (splitterControl!=null)
		                	splitterControl.waitFor();
		                splitterControl=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, to, 6);
		                if (splitterData!=null)
		                	splitterData.waitFor();
		        		splitterData=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, to, 7);
	        		}
    			}
    		}
    		else if (contains(myrank,sentimentNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toSentiment.receiveStarts();
    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			//System.out.println("Before receive info sentiment "+myrank);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 16);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info sentiment "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toSentiment.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingSentiments--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingSentiments==0){
    					aggregatedMessage[1]=myrank;
    					if (sentimentControl!=null)
    						sentimentControl.waitFor();
    					controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, indexNodes[j], 18);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toSentiment.receiveStarts();
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
    				//System.out.println("Before receive data sentiment "+myrank);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 17);
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data sentiment "+myrank);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toSentiment.receiveEnds());
	        		Output out=sentiment.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                aggregatedMessage[0]=sendBytes.length;
		                aggregatedMessage[1]=myrank;

		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            int to=hash%indexes;
        	            if (to<0)
        	            	to+=indexes;
        	            to+=indexNodes[0];
        	            
        	            controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
        	            dataBuffer=ByteBuffer.wrap(sendBytes);
        	            
        	            if (sentimentControl!=null)
        	            	sentimentControl.waitFor();
        	            sentimentControl=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, to, 18);
        	            if (sentimentData!=null)
        	            	sentimentData.waitFor();
		        		sentimentData=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, to, 19);
	        		}
    			}

    		}
    		else if (contains(myrank,tokenizerNodes)){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toTokenizer.receiveStarts();
    			ByteBuffer controlBuffer=ByteBuffer.allocateDirect(8);
    			//System.out.println("Before receive info tokenize "+myrank);
    			Request controlArrived=MPI.COMM_WORLD.iRecv(controlBuffer, 8, MPI.BYTE, MPI.ANY_SOURCE, 2);
    			controlArrived.waitFor();
    			controlBuffer.get(bytesMessage);
    			aggregatedMessage=bytesToInt(bytesMessage);
    			//System.out.println("After receive info tokenize "+myrank);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toTokenizer.receiveEnds());
    			if (aggregatedMessage[0]==-1){
    				waitingTokenizers--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingTokenizers==0){
    					aggregatedMessage[1]=myrank;
    					if (tokenizerControl!=null)
    						tokenizerControl.waitFor();
    					controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
    					for (int j=0;j<splitterNodes.length;j++)
            				MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, splitterNodes[j], 4);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[aggregatedMessage[0]];
    				if (monitorThisTime)
        				toTokenizer.receiveStarts();
    				ByteBuffer dataBuffer=ByteBuffer.allocateDirect(aggregatedMessage[0]);
    				//System.out.println("Before receive data tokenize "+myrank);
	        		Request dataArrived=MPI.COMM_WORLD.iRecv(dataBuffer, aggregatedMessage[0], MPI.BYTE, aggregatedMessage[1], 3);	
	        		dataArrived.waitFor();
	        		dataBuffer.get(bytes);
	        		//System.out.println("After receive data tokenize "+myrank);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toTokenizer.receiveEnds());
	        		Output out=tokenizer.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                aggregatedMessage[0]=sendBytes.length;
		                aggregatedMessage[1]=myrank;
		                int to=rn.nextInt(splitters)+splitterNodes[0];
		                
		                controlBuffer.put(intsToByteArray(aggregatedMessage),0,8);
		                dataBuffer=ByteBuffer.wrap(sendBytes);
		                
		                if (tokenizerControl!=null)
		                	tokenizerControl.waitFor();
		                tokenizerControl=MPI.COMM_WORLD.iSend(controlBuffer, 8, MPI.BYTE, to, 4);
		                if (tokenizerData!=null)
		                	tokenizerData.waitFor();
		        		tokenizerData=MPI.COMM_WORLD.iSend(dataBuffer, sendBytes.length, MPI.BYTE, to, 5);
	        		}
    			}
    		}
        	counter++;
        }

        MPI.Finalize();
    }
}
