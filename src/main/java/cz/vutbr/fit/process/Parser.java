package cz.vutbr.fit.process;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.util.CoreMap;

public class Parser {
	private Annotator parser;
	
	public Parser(Annotator parser){
		this.parser=parser;
	}
	
	public Output execute(Output in){
		ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
		
		for (int i=0;i<tweets.size();i++){
			parser.annotate(tweets.get(i).getTextAnnot());
		}
		Output out=new Output("parse",tweets,in.getId());
		return out;
	}

}
