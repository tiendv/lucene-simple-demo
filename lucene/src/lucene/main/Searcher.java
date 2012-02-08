/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package lucene.main;

import lucene.properties.Config;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;

/**
 *
 * @author duchuynh
 */
public class Searcher {
    private IndexSearcher searcher = null;

    public Searcher(){
        try{
            searcher = new IndexSearcher(Common.getFSDirectory(Config.getParameter("index")));            
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    /**
     * @author : Huynh Duc
     */
    public void search(String q){
        try{
            QueryParser parser = new QueryParser(Version.LUCENE_34,Config.getParameter("field.name"),new StandardAnalyzer(Version.LUCENE_34));
            Query query = parser.parse(q);
            TopDocs results = searcher.search(query,50);
            System.out.println("total hits: " + results.totalHits);
            ScoreDoc[] hits = results.scoreDocs;
            for (ScoreDoc hit : hits) {
                Document doc = searcher.doc(hit.doc);
                System.out.println("id : " + doc.get(Config.getParameter("field.id")) + " name : "+ doc.get(Config.getParameter("field.name")));
            }
            searcher.close();
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] agrs){
        Searcher searcher = new Searcher();
        searcher.search("dog");
    }
}
