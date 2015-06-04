package cz.vutbr.fit.entrypoint;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;
import net.jpountz.xxhash.XXHash32;
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
	private static final int TO_FILTER_INFO=0;
	private static final int TO_FILTER_DATA=1;
	private static final int TO_TOKENIZER_INFO=2;
	private static final int TO_TOKENIZER_DATA=3;
	private static final int TO_SPLITTER_INFO=4;
	private static final int TO_SPLITTER_DATA=5;
	private static final int TO_POS_INFO=6;
	private static final int TO_POS_DATA=7;
	private static final int TO_GENDER_INFO=8;
	private static final int TO_GENDER_DATA=9;
	private static final int TO_LEMMA_INFO=10;
	private static final int TO_LEMMA_DATA=11;
	private static final int TO_NER_INFO=12;
	private static final int TO_NER_DATA=13;
	private static final int TO_PARSER_INFO=14;
	private static final int TO_PARSER_DATA=15;
	private static final int TO_SENTIMENT_INFO=16;
	private static final int TO_SENTIMENT_DATA=17;
	private static final int TO_INDEX_INFO=18;
	private static final int TO_INDEX_DATA=19;
	

	/**
	 * Checks whether given rank is contained in given array
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
		
		//Example usage:
		//mpirun -np 24 -host 147.229.8.104 /usr/lib/jvm/java-8-oracle/bin/java -jar twittermpi-jar-with-dependencies.jar -d 4 -f 5 -g 1 -x 12 -l 2 -n 4 -p 8 -o 3 -s 1 -e 2 -t 2 -r 10 : -np 8 -host 147.229.8.105 /usr/lib/jvm/java-8-oracle/bin/java -jar twittermpi-jar-with-dependencies.jar -d 4 -f 5 -g 1 -x 12 -l 2 -n 4 -p 8 -o 3 -s 1 -e 2 -t 2 -r 10  : -np 12 -host 147.229.8.106 /usr/lib/jvm/java-8-oracle/bin/java -jar twittermpi-jar-with-dependencies.jar -d 4 -f 5 -g 1 -x 12 -l 2 -n 4 -p 8 -o 3 -s 1 -e 2 -t 2 -r 10
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
		
		//getting parameters
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
		
		//how much acknowledges should be received before executor can finish
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
		
		//ranks of particular executors
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
		
		//initializing executors' ranks
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
		
		//random for shuffle grouping
		Random rn = new Random();
		
		//hashing for fields grouping
		XXHashFactory factory = XXHashFactory.fastestInstance();
		XXHash32 hasher=factory.hash32();
		int seed = 0x9747b28c;
		
		//find out which processing step should be done in this rank
		boolean isDump=contains(myrank,dumpNodes);
		boolean isFilter=contains(myrank,filterNodes);
		boolean isGender=contains(myrank,genderNodes);
		boolean isIndex=contains(myrank,indexNodes);
		boolean isLemma=contains(myrank,lemmaNodes);
		boolean isNer=contains(myrank,nerNodes);
		boolean isParser=contains(myrank,parserNodes);
		boolean isPoS=contains(myrank,posNodes);
		boolean isSplitter=contains(myrank,splitterNodes);
		boolean isSentiment=contains(myrank,sentimentNodes);
		boolean isTokenizer=contains(myrank,tokenizerNodes);
		
		//initialize processing pipeline
		Properties props=new Properties();
		props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment, gender, ner");
		StanfordCoreNLP pipeline=new StanfordCoreNLP(props);
		
		//initialize executors
		if (isDump){
	        dump=new Dump(granularity,myrank);
		}
		else if (isFilter){
			filter=new Filter();
		}
		else if (isGender){
			gender=new Gender(pipeline.getExistingAnnotator("gender"));
		}
		else if (isIndex){
			index=new Index();
		}
		else if (isLemma){
			lemma=new Lemma(pipeline.getExistingAnnotator("lemma"));
		}
		else if (isNer){
			ner=new Ner(pipeline.getExistingAnnotator("ner"));
		}
		else if (isParser){
			parser=new Parser(pipeline.getExistingAnnotator("parse"));
		}
		else if (isPoS){
			pos=new POSTagger(pipeline.getExistingAnnotator("pos"));
		}
		else if (isSplitter){
			splitter=new SentenceSplitter(pipeline.getExistingAnnotator("ssplit"));
		}
		else if (isSentiment){
			sentiment=new Sentiment(pipeline.getExistingAnnotator("sentiment"));
		}
		else if (isTokenizer){
			tokenizer=new Tokenizer(pipeline.getExistingAnnotator("tokenize"));
		}
		
        //info array    
        int infoMessage[]=new int[2];
        
        //monitoring
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
        
        //for sparse monitoring - not used
        long monitor=1000/granularity;
        long counter=0;

        while (true){
        	//boolean monitorThisTime=counter%monitor==0;
        	boolean monitorThisTime=true;
        	
        	//dump executor
        	if (isDump){
        		Output out=dump.nextData();
        		if (out!=null){
	        		byte[] bytes=obj2array(out);
	                infoMessage[0]=bytes.length;
	                infoMessage[1]=myrank;
	        		int to=rn.nextInt(filters)+filterNodes[0];
	        		MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_FILTER_INFO);
	        		MPI.COMM_WORLD.send(bytes, bytes.length, MPI.BYTE, to, TO_FILTER_DATA);

        		}
        		else{
        			infoMessage[0]=-1;
        			infoMessage[1]=myrank;
        			for (int j=0;j<filterNodes.length;j++)
        				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, filterNodes[j], TO_FILTER_INFO);
        			break;
        		}
    		}
        	//filter executor
    		else if (isFilter){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toFilter.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_FILTER_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toFilter.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingFilters--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingFilters==0){
    					infoMessage[1]=myrank;
    					for (int j=0;j<tokenizerNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, tokenizerNodes[j], TO_TOKENIZER_INFO);
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, indexNodes[j], TO_INDEX_INFO);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toFilter.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_FILTER_DATA);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	        			programInstanceSensor.subtract(toFilter.receiveEnds());
	        		Output out=filter.execute(in);
	        		if (monitorThisTime)
	    				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                infoMessage[0]=sendBytes.length;
		                infoMessage[1]=myrank;
		                int to=rn.nextInt(tokenizers)+tokenizerNodes[0];
		                
		                MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_TOKENIZER_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_TOKENIZER_DATA);
		        		
		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            to=hash%indexes;
        	            if (to<0)
        	            	to+=indexes;
        	            to+=indexNodes[0];
        	            
        	            MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_INDEX_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_INDEX_DATA);
	        		}
    			}
    		}
        	//gender executor
    		else if (isGender){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toGender.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_GENDER_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toGender.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingGenders--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingGenders==0){
    					infoMessage[1]=myrank;
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, indexNodes[j], TO_INDEX_INFO);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toGender.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_GENDER_DATA);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toGender.receiveEnds());
	        		Output out=gender.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                infoMessage[0]=sendBytes.length;
		                infoMessage[1]=myrank;

		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            int to=hash%indexes;
        	            if (to<0)
        	            	to+=indexes;
        	            to+=indexNodes[0];
        	            MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_INDEX_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_INDEX_DATA);
	        		}
    			}
    		}
        	//index executor
    		else if (isIndex){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toIndex.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_INDEX_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toIndex.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingIndexes--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingIndexes==0){
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toIndex.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_INDEX_DATA);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toIndex.receiveEnds());
	        		index.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
    			}
    		}
        	//lemma executor
    		else if (isLemma){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toLemma.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_LEMMA_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toLemma.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingLemmas--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingLemmas==0){
    					infoMessage[1]=myrank;
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, indexNodes[j], TO_INDEX_INFO);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toLemma.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_LEMMA_DATA);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toLemma.receiveEnds());
	        		Output out=lemma.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                infoMessage[0]=sendBytes.length;
		                infoMessage[1]=myrank;

		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            int to=hash%indexes;
        	            if (to<0)
        	            	to+=indexes;
        	            to+=indexNodes[0];
        	            MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_INDEX_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_INDEX_DATA);
	        		}
    			}
    		}
        	//ner executor
    		else if (isNer){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toNer.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_NER_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toNer.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingNers--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingNers==0){
    					infoMessage[1]=myrank;
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, indexNodes[j], TO_INDEX_INFO);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toNer.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_NER_DATA);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toNer.receiveEnds());
	        		Output out=ner.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                infoMessage[0]=sendBytes.length;
		                infoMessage[1]=myrank;

		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            int to=hash%indexes;
        	            if (to<0)
        	            	to+=indexes;
        	            to+=indexNodes[0];
        	            MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_INDEX_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_INDEX_DATA);
	        		}
    			}
    		}
        	//parser executor
    		else if (isParser){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toParser.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_PARSER_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toParser.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingParsers--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingParsers==0){
    					infoMessage[1]=myrank;
    					for (int j=0;j<sentimentNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, sentimentNodes[j], TO_SENTIMENT_INFO);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toParser.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_PARSER_DATA);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toParser.receiveEnds());
	        		Output out=parser.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                infoMessage[0]=sendBytes.length;
		                infoMessage[1]=myrank;

		        		int to = rn.nextInt(sentiments)+sentimentNodes[0];
        	            MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_SENTIMENT_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_SENTIMENT_DATA);
	        		}
    			}

    		}
        	//PoS executor
    		else if (isPoS){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toPos.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_POS_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toPos.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingPoses--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingPoses==0){
    					infoMessage[1]=myrank;
    					for (int j=0;j<genderNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, genderNodes[j], TO_GENDER_INFO);
    					for (int j=0;j<lemmaNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, lemmaNodes[j], TO_LEMMA_INFO);
    					for (int j=0;j<nerNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, nerNodes[j], TO_NER_INFO);
    					for (int j=0;j<parserNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, parserNodes[j], TO_PARSER_INFO);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toPos.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_POS_DATA);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toPos.receiveEnds());
	        		Output out=pos.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                infoMessage[0]=sendBytes.length;
		                infoMessage[1]=myrank;
		                int to=rn.nextInt(genders)+genderNodes[0];
		                
		                MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_GENDER_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_GENDER_DATA);
		        		
		        		to = rn.nextInt(lemmas)+lemmaNodes[0];
		        		MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_LEMMA_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_LEMMA_DATA);
		        		
		        		to = rn.nextInt(ners)+nerNodes[0];
		        		MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_NER_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_NER_DATA);
		        		
		        		to = rn.nextInt(parsers)+parserNodes[0];
		        		MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_PARSER_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_PARSER_DATA);
	        		}
    			}
    		}
        	//splitter executor
    		else if (isSplitter){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toSplit.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_SPLITTER_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toSplit.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingSplitters--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingSplitters==0){
    					infoMessage[1]=myrank;
    					for (int j=0;j<posNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, posNodes[j], TO_POS_INFO);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toSplit.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_SPLITTER_DATA);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toSplit.receiveEnds());
	        		Output out=splitter.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                infoMessage[0]=sendBytes.length;
		                infoMessage[1]=myrank;
		                int to=rn.nextInt(poses)+posNodes[0];
		                
		                MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_POS_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_POS_DATA);
	        		}
    			}
    		}
        	//sentiment executor
    		else if (isSentiment){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toSentiment.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_SENTIMENT_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toSentiment.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingSentiments--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingSentiments==0){
    					infoMessage[1]=myrank;
    					for (int j=0;j<indexNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, indexNodes[j], TO_INDEX_INFO);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toSentiment.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_SENTIMENT_DATA);
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toSentiment.receiveEnds());
	        		Output out=sentiment.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                infoMessage[0]=sendBytes.length;
		                infoMessage[1]=myrank;

		        		String hashId=out.getId();
		        		byte[] hashBytes=hashId.getBytes();
        	            int hash=hasher.hash(hashBytes, 0,hashBytes.length,seed);
        	            int to=hash%indexes;
        	            if (to<0)
        	            	to+=indexes;
        	            to+=indexNodes[0];
        	            MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_INDEX_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_INDEX_DATA);
	        		}
    			}

    		}
        	//tokenizer executor
    		else if (isTokenizer){
    			if (monitorThisTime)
    				programInstanceSensor.programStarts();
    			if (monitorThisTime)
    				toTokenizer.receiveStarts();
    			MPI.COMM_WORLD.recv(infoMessage, 2, MPI.INT, MPI.ANY_SOURCE, TO_TOKENIZER_INFO);
    			if (monitorThisTime)
    				programInstanceSensor.subtract(toTokenizer.receiveEnds());
    			if (infoMessage[0]==-1){
    				waitingTokenizers--;
    				if (monitorThisTime)
        				programInstanceSensor.programEnds();
    				if (waitingTokenizers==0){
    					infoMessage[1]=myrank;
    					for (int j=0;j<splitterNodes.length;j++)
            				MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, splitterNodes[j], TO_SPLITTER_INFO);
            			break;
    				}
    			}
    			else{
    				byte[] bytes=new byte[infoMessage[0]];
    				if (monitorThisTime)
        				toTokenizer.receiveStarts();
	        		MPI.COMM_WORLD.recv(bytes, infoMessage[0], MPI.BYTE, infoMessage[1], TO_TOKENIZER_DATA);	
	        		Output in=(Output)array2obj(bytes);
	        		if (monitorThisTime)
	    				programInstanceSensor.subtract(toTokenizer.receiveEnds());
	        		Output out=tokenizer.execute(in);
	        		if (monitorThisTime)
        				programInstanceSensor.programEnds();
	        		if (out!=null){
	        			byte[] sendBytes = obj2array(out);
		                infoMessage[0]=sendBytes.length;
		                infoMessage[1]=myrank;
		                int to=rn.nextInt(splitters)+splitterNodes[0];
		                
		                MPI.COMM_WORLD.send(infoMessage, 2, MPI.INT, to, TO_SPLITTER_INFO);
		        		MPI.COMM_WORLD.send(sendBytes, sendBytes.length, MPI.BYTE, to, TO_SPLITTER_DATA);
	        		}
    			}
    		}
        	counter++;
        }

        MPI.Finalize();
    }
}
