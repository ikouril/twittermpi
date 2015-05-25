package cz.vutbr.fit.process;

import java.util.List;
import java.util.UUID;

import cz.vutbr.fit.util.Output;
import cz.vutbr.fit.util.Tweet;
import edu.stanford.nlp.pipeline.Annotator;

public class POSTagger {
	private Annotator posTagger;
	
	public POSTagger(Annotator posTagger){
		this.posTagger=posTagger;
	}
	
	public Output execute(Output in){
		List<Tweet> data=(List<Tweet>) in.getData();
		for (int i=0;i<data.size();i++){
			posTagger.annotate(data.get(i).getAuthorAnnot());
			posTagger.annotate(data.get(i).getTextAnnot());
		}
		Output out=new Output("pos",data,in.getId());
		return out;
	}

}
