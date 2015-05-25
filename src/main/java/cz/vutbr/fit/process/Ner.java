package cz.vutbr.fit.process;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.util.CoreMap;

public class Ner {
	private Annotator nerTagger;
	
	public Ner(Annotator nerTagger){
		this.nerTagger=nerTagger;
	}
	
	public Output execute(Output in){
		ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
		
		for (int i=0;i<tweets.size();i++){
			nerTagger.annotate(tweets.get(i).getTextAnnot());
			
			String actualAnnot="";
			String actualPerson="";
			String allPersons="";
			
			List<CoreMap> sentences =tweets.get(i).getTextAnnot().get(CoreAnnotations.SentencesAnnotation.class);
			for (CoreMap sentence : sentences) {
			  for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
				  actualAnnot=token.get(NamedEntityTagAnnotation.class);
				  if (actualAnnot.equals("PERSON")){
					  String actualToken=token.originalText();
					  if (!actualPerson.isEmpty())
						  actualPerson+=" "+actualToken;
					  else
						  actualPerson=actualToken;
				  }
				  else{
					  if (!actualPerson.isEmpty()){
						  if (!allPersons.isEmpty())
							  allPersons+=", "+actualPerson;
						  else
							  allPersons=actualPerson;
						  actualPerson="";
					  }
				  }
	            }
			}
			
			if (actualAnnot.equals("PERSON")){
				if (!allPersons.isEmpty())
					allPersons+=", "+actualPerson;
				else
					allPersons=actualPerson;
			}
			tweets.get(i).setPerson(allPersons);
		}
		Output out=new Output("ner",tweets,in.getId());
		return out;
	}
}
