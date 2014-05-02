package myudfs;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.hadoop.fs.*;
import java.io.DataInputStream;
import org.apache.pig.impl.util.UDFContext;

public class CATEGORIZE extends EvalFunc<DataBag> { 
    TreeMap<String, String> wikimap = null;
    String wikifile;
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();

    public CATEGORIZE(String file) {
      wikifile = file;
    }


    @Override
    public DataBag exec(Tuple input) throws IOException {
      if (wikimap == null) {
        wikimap = new TreeMap<String, String>();
        FileSystem fs = FileSystem.get(UDFContext.getUDFContext().getJobConf());
        DataInputStream in = fs.open(new Path(wikifile));
        String line;
        while ((line = in.readLine()) != null) {
          String[] toks = line.split("\t", 2);
          wikimap.put(toks[0].toLowerCase(), toks[1].toLowerCase());
        }
      }
      try {
        DataBag output = bagFactory.newDefaultBag();
        String title_text = (String) input.get(0);
        String[] keywords = title_text.split(" "); //assume they are all lowercase
        String result = "";
        for (int i = 0; i < keywords.length; i++) {
          String keyword = keywords[i].toLowerCase().replaceAll("[^A-Za-z0-9 ]", "");
          String child_category = wikimap.get(keyword);
          if (child_category != null) {
            output.add(tupleFactory.newTuple(keyword));
          }
          if ((i + 1) < keywords.length) {
	    String keyword2 = keyword + " " + keywords[i + 1].toLowerCase().replaceAll("[^A-Za-z0-9 ]", "");
            String category2 = wikimap.get(keyword2);
            if (category2 != null) {
              output.add(tupleFactory.newTuple(keyword2));
            }
          }
        }
        return output;
      } 
      catch (Exception e) {
        throw new RuntimeException("CATEGORIZE error", e);
      }
    }
}

