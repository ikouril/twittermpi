package cz.vutbr.fit.process;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import edu.stanford.nlp.ie.machinereading.structure.MachineReadingAnnotations.GenderAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.util.CoreMap;

public class Gender {
	private Annotator genderClassifier;
	
	public Gender(Annotator genderClassifier){
		this.genderClassifier=genderClassifier;
	}
	
	public Output execute(Output in){
		if (in==null)
			return null;
		ArrayList<Tweet> tweets=(ArrayList<Tweet>) in.getData();
		for (int i=0;i<tweets.size();i++){
			genderClassifier.annotate(tweets.get(i).getAuthorAnnot());
			boolean genderResolved=false;
			String gender="unresolved";
			List<CoreMap> sentences = tweets.get(i).getAuthorAnnot().get(CoreAnnotations.SentencesAnnotation.class);
			for (CoreMap sentence : sentences) {
			  for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
				  String result = token.get(GenderAnnotation.class);
				  if (result!=null){
					  gender=result.toLowerCase();
					  genderResolved=true;
					  break;
				  }
	            }
			  if (genderResolved)
				  break;
			}
			tweets.get(i).setGender(gender);
		}
		Output out=new Output("gender",tweets,in.getId());
		return out;
	}
	

}
