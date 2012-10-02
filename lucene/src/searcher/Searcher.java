/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package searcher;

import constant.Common;
import constant.IndexConst;
import dto.PaperDTO;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 *
 * @author duchuynh
 */
public class Searcher {

    private IndexSearcher searcher = null;

    public Searcher() {
        try {
            searcher = new IndexSearcher(Common.getFSDirectory(IndexConst.PAPER_INDEX_PATH));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @author : Huynh Duc
     */
    public void search(PaperDTO paperSearch) {
        try {
//            BooleanQuery advancedSearch = new BooleanQuery();
//            
//            QueryParser parserTitle = new QueryParser(Version.LUCENE_34,Config.getParameter("field.title"),new StandardAnalyzer(Version.LUCENE_34));            
//            Query queryTitle = parserTitle.parse(paperSearch.title);            
//            advancedSearch.add(queryTitle, Occur.MUST);
//            
//            QueryParser parserAuthor = new QueryParser(Version.LUCENE_34,Config.getParameter("field.conferenceName"),new KeywordAnalyzer());            
//            Query queryAuthor = parserAuthor.parse(paperSearch.conferenceName);            
//            advancedSearch.add(queryAuthor, Occur.MUST);

//            String[] multiField = {Config.getParameter("field.title"), Config.getParameter("field.conferenceName")};
//            MultiFieldQueryParser parser = new MultiFieldQueryParser(Version.LUCENE_34, multiField, new StandardAnalyzer(Version.LUCENE_34));
//            Query query = parser.parse(paperSearch.title);
//            BooleanQuery advancedSearch = new BooleanQuery();
//            advancedSearch.add(query, Occur.MUST);
            BooleanQuery advanced1Search = new BooleanQuery();

            String[] termList = paperSearch.title.split(" ");
            if (termList.length > 0) {
                PhraseQuery advancedSearch = new PhraseQuery();
                for (int i = 0; i < termList.length; i++) {
                    Term term = new Term(IndexConst.PAPER_TITLE_FIELD, termList[i]);
                    advancedSearch.add(term);
                }
                advancedSearch.setSlop(4);
                advanced1Search.add(advancedSearch, Occur.SHOULD);
            }
            PhraseQuery advanced2Search = new PhraseQuery();
            String[] termConferenceList = paperSearch.authors.split(" ");
            for (int i = 0; i < termConferenceList.length; i++) {
                Term term = new Term(IndexConst.PAPER_AUTHORSNAME_FIELD, termConferenceList[i]);
                advanced2Search.add(term);
            }
            advanced2Search.setSlop(4);
            advanced1Search.add(advanced2Search, Occur.MUST);

            Filter yearFilter = NumericRangeFilter.newIntRange(IndexConst.PAPER_YEAR_FIELD, 2005, 2008, true, true);

            System.out.println(advanced1Search.toString());
            TopDocs results = searcher.search(advanced1Search, yearFilter, 50);
            System.out.println("total hits: " + results.totalHits);
            ScoreDoc[] hits = results.scoreDocs;
            for (ScoreDoc hit : hits) {
                Document doc = searcher.doc(hit.doc);
                System.out.println("idPaper : " + doc.get(IndexConst.PAPER_IDPAPER_FIELD)
                        + " title : " + doc.get(IndexConst.PAPER_TITLE_FIELD)
                        + " + authorName : " + doc.get(IndexConst.PAPER_AUTHORSNAME_FIELD)
                        + " + Year : " + doc.get(IndexConst.PAPER_YEAR_FIELD));
            }
            searcher.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] agrs) {
        Searcher searcher = new Searcher();
        PaperDTO objectSearch = new PaperDTO();
        objectSearch.title = "data";
        objectSearch.authors = "amir";
        searcher.search(objectSearch);
    }
}
