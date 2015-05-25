package cz.vutbr.fit.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import edu.stanford.nlp.pipeline.Annotation;

public class Filter {
	private static final int VARIANT=0;
	private static final int PART=1;
	private static final int DATADISC=2;
	
	public final int GAMES=195;
	public int BLOCK;
	public String FILES;
	public String[] keywords=new String[GAMES];
	public String[][] variants=new String[GAMES][];
	public String[][] parts=new String[GAMES][];
	public String[][] datadiscs=new String[GAMES][];
	public Map<String,Integer> gameMap=new HashMap<String,Integer>();
	public Pattern pattern;
	
	public Filter(){
		StringBuilder patternBuilder=new StringBuilder();
        try{
	        BufferedReader reader=new BufferedReader(new InputStreamReader(new URL("http://athena3.fit.vutbr.cz/twitterstorm/allgames.txt").openStream()));
			String line=reader.readLine();
			int cnt=0;
			patternBuilder.append("(");
			while (line!=null){
				keywords[cnt]=line;
				gameMap.put(line, cnt);
				cnt++;
				patternBuilder.append(line+"|");
				line=reader.readLine();
			}
			reader.close();
        }
        catch (Exception e){
        	e.printStackTrace();
        }
        int variantCounter=0;
        try{
	        BufferedReader reader=new BufferedReader(new InputStreamReader(new URL("http://athena3.fit.vutbr.cz/twitterstorm/name_variations.txt").openStream()));
			String line=reader.readLine();
			while (line!=null){
				List<String> vals=new ArrayList<String>();
				String[] vars=line.split("\t");
				for (String var:vars){
					vals.add(var);
				}
				Collections.sort(vals, new Comparator<String>() {

					@Override
					public int compare(String o1, String o2) {
						return o2.length() - o1.length();
					}
				});
				String[] resultArray=new String[vals.size()];
				variants[variantCounter++]=vals.toArray(resultArray);
				line=reader.readLine();
			}
			reader.close();
        }
        catch (Exception e){
        	e.printStackTrace();
        }
        int partCounter=0;
        try{
	        BufferedReader reader=new BufferedReader(new InputStreamReader(new URL("http://athena3.fit.vutbr.cz/twitterstorm/name_part_variations.txt").openStream()));
			String line=reader.readLine();
			while (line!=null){
				if (line.length()>0){
					List<String> vals=new ArrayList<String>();
					String[] vars=line.split("\t");
					for (String var:vars){
						vals.add(var);
					}
					Collections.sort(vals, new Comparator<String>() {

						@Override
						public int compare(String o1, String o2) {
							return o2.length() - o1.length();
						}
					});
					String[] resultArray=new String[vals.size()];
					parts[partCounter++]=vals.toArray(resultArray);
				}
				else{
					parts[partCounter++]=null;
				}
				line=reader.readLine();
			}
			reader.close();
        }
        catch (Exception e){
        	e.printStackTrace();
        }
        
        int datadiscCounter=0;
        try{
	        BufferedReader reader=new BufferedReader(new InputStreamReader(new URL("http://athena3.fit.vutbr.cz/twitterstorm/datadisc_variants.txt").openStream()));
			String line=reader.readLine();
			while (line!=null){
				if (line.length()>0){
					List<String> vals=new ArrayList<String>();
					String[] vars=line.split("\t");
					for (String var:vars){
						vals.add(var);
					}
					Collections.sort(vals, new Comparator<String>() {

						@Override
						public int compare(String o1, String o2) {
							return o2.length() - o1.length();
						}
					});
					String[] resultArray=new String[vals.size()];
					datadiscs[datadiscCounter++]=vals.toArray(resultArray);
				}
				else{
					datadiscs[datadiscCounter++]=null;
				}
				line=reader.readLine();
			}
			reader.close();
        }
        catch (Exception e){
        	e.printStackTrace();
        }
        patternBuilder.setCharAt(patternBuilder.length()-1, ')');
        pattern=Pattern.compile(patternBuilder.toString());
	}
	
	private String find(int gameId,String line,int type){
		String[] source=null;
		if (type==VARIANT)
			source=variants[gameId];
		else if (type==PART)
			source=parts[gameId];
		else
			source=datadiscs[gameId];

		if (source!=null){
			StringBuilder builder=new StringBuilder();
			builder.append("(");
			builder.append(StringUtils.join(source,"|"));
			builder.append(")");
			
			Pattern p=Pattern.compile(builder.toString(),Pattern.CASE_INSENSITIVE);
			Matcher m=p.matcher(line);
			String match="";
			
			//find longest match
			while (m.find()){
				String newMatch=m.group(1);
				if (newMatch.length()>match.length())
					match=newMatch;
			}
			if (!match.isEmpty())
				return match;
		}
		
		return null;
	}
	
	public Output execute(Output in){

		ArrayList<Tweet> block=(ArrayList<Tweet>) in.getData();
		//Tweet tweet=(Tweet) input.getValue(0);
		ArrayList<Tweet> tweets=new ArrayList<Tweet>();
		for (Tweet tweet:block){
		
			String text=tweet.getText();
			Matcher m=pattern.matcher(text);
	    	StringBuilder output=new StringBuilder();
	    	StringBuilder datadiscOutput=new StringBuilder();
	    	StringBuilder partOutput=new StringBuilder();
	    	StringBuilder variantOutput=new StringBuilder();
	    	Set<String> games=new HashSet<String>();
	    	Map<String,String>datadiscs=new HashMap<String,String>();
	    	Map<String,String>parts=new HashMap<String,String>();
	    	Map<String,String>variants=new HashMap<String,String>();
	        while (m.find()) {
	        	String game=m.group(1);
	        	int gameId=gameMap.get(game);
	        	String datadisc=find(gameId,text,DATADISC);
	        	String part=find(gameId,text,PART);
	        	String variant=find(gameId,text,VARIANT);
	        	datadiscs.put(game,datadisc);
	        	parts.put(game, part);
	        	variants.put(game, variant);
	            games.add(game);
	        }
	        
	    	if (games.size()>0){
	    		
	    		
	    		Annotation textAnnot=new Annotation(text);
	    		String author=tweet.getAuthor();
	    		Annotation authorAnnot=new Annotation(author);
	    		
	    		tweet.setTextAnnot(textAnnot);
	    		tweet.setAuthorAnnot(authorAnnot);
	    		
	    		for (String game:games){
	            	if (output.length()!=0)
	            		output.append(", ");
	                output.append(game);
	                
	                if (datadiscOutput.length()!=0)
	                	datadiscOutput.append(", ");
	                
	                String datadisc=datadiscs.get(game);
	                if (datadisc!=null)
	                	datadiscOutput.append(datadisc);
	                
	                if (variantOutput.length()!=0)
	                	variantOutput.append(", ");
	                
	                String variant=variants.get(game);
	                if (variant!=null)
	                	variantOutput.append(variant);
	                
	                if (partOutput.length()!=0)
	                	partOutput.append(", ");
	                
	                String part=parts.get(game);
	                if (part!=null)
	                	partOutput.append(part);
	                
	                
	            }
	    		tweet.setGame(output.toString());
	    		tweet.setPart(partOutput.toString());
	    		tweet.setDatadisc(datadiscOutput.toString());
	    		tweet.setVariant(variantOutput.toString());
	    		tweets.add(tweet);	
	    	}
		}
		if (tweets.size()>0){
			Output out=new Output("filter",tweets,in.getId());
			return out;
		}
		else{
			return null;
		}
	}

}
