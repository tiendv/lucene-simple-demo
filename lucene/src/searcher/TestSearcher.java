package searcher;

import constant.Common;
import constant.IndexConst;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class TestSearcher {
    // Lucene field

    //public static final String[] FIELDS = {"title", "authorName", "conferenceName", "journalName"};
    public static final String FIELD = IndexConst.AUTHOR_IDAUTHOR_FIELD;
    public static final String KEYWORD = "8";
    //public static final String[] KEYWORDS = {"100", "101"};

    public static void main(String[] args) throws Exception {
        try {
            // 1. Query
            BooleanQuery query;

            query = searchIndex(FIELD, KEYWORD);

            //query = searchIndexWithMultiField(FIELDS, KEYWORD);

            /*
             * Truy vấn phù hợp với các văn bản có chứa một thuật ngữ, một từ
             * với field tưng ứng.
             */
            //query = searchIndexWithTermQuery(Common.FIELD, Common.KEYWORD);

            /*
             * Tìm kiếm trong index có cứa một cụm từ với field tương ứng với
             * một trình tự cụ thể
             */
            //query = searchIndexWithPhraseQuery(FIELD, KEYWORDS);

            /*
             * Truy vấn có thể với từ sai chính tả, từ gần đúng với field tưng
             * ứng.
             */
            //query = searchIndexWithFuzzyQuery(FIELD, KEYWORD);

            /*
             * Truy vấn với tiền tố (prefix) với field tưng ứng.
             */
            //query = searchIndexWithPrefixQuery(FIELD, KEYWORD);

            /*
             * Tìm kiếm trong index có cứa một cụm từ với field tương ứng không
             * cần trình tự cụ thể
             */
            //query = searchIndexWithBooleanQuery(FIELD, KEYWORDS);

            /*
             * Tìm kiếm với từ khóa (keyword*) trong field tưng ứng
             */
            //query = searchIndexWithWildCardQuery(Common.FIELD, Common.KEYWORD);

            // 2. Search
            File indexDir = new File(IndexConst.AUTHOR_INDEX_PATH);
            Directory directory = FSDirectory.open(indexDir);
            IndexSearcher searcher = new IndexSearcher(directory);
            //IndexSearcher searcher = new IndexSearcher(Common.getFSDirectory(Common.INDEX_LUCENE_PATH));

            // 3. display results
            //Filter idFilter = NumericRangeFilter.newIntRange("id", 0, 54, true, true);
            //TopDocs result = searcher.search(query, idFilter, Integer.MAX_VALUE);

            TopDocs result = searcher.search(query, Integer.MAX_VALUE);
            if (result != null) {
                ScoreDoc[] hits = result.scoreDocs;
                System.out.println("Found " + result.totalHits + " hits.");
                int start = 0;
                int end = start + 10;
                if (end > result.totalHits) {
                    end = result.totalHits;
                }
                int i = 0;
               // for (int i = start; i < end; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = searcher.doc(hit.doc);
                    System.out.println((i + 1) + ".name:" + doc.get(IndexConst.AUTHOR_AUTHORNAME_FIELD));
                    /*JSONParser parser = new JSONParser();
                     JSONObject jauthors = (JSONObject) parser.parse(doc.get(IndexConst.PAPER_AUTHORS_FIELD));
                     JSONArray jauthor = (JSONArray) jauthors.get("authors");
                     for(int j=0; j<jauthor.size(); j++){
                     System.out.println("authorName:" + jauthor.get(j));
                     JSONObject temp = (JSONObject) jauthor.get(j);
                     System.out.println(temp.get("idAuthor") + "+" + temp.get("authorName"));
                     }
                     */

                    // Lấy giá trị PublicationCitation
                    ArrayList<Object> listPublicationCitation = (ArrayList<Object>) Common.SToO(doc.get(IndexConst.AUTHOR_LISTPUBLICATIONCITATION_FIELD));
                    Iterator it = listPublicationCitation.iterator();
                    while (it.hasNext()) {
                        LinkedHashMap<String, Integer> temp = (LinkedHashMap<String, Integer>) it.next();
                        System.out.println("year:" + temp.get("year") + "-citation:" + temp.get("citation") + "-publication:" + temp.get("publication"));
                    }
                    
                    System.out.println();
                    
                    // Lấy giá trị RankSubdomain
                    LinkedHashMap<Integer, Object> listRankSubdomain = (LinkedHashMap<Integer, Object>) Common.SToO(doc.get(IndexConst.AUTHOR_LISTRANKSUBDOMAIN_FIELD));
                    // Lấy giá trị RankSubdomain với idSubdomain=1
                    LinkedHashMap<String, Integer> rankSubdomain = (LinkedHashMap<String, Integer>) listRankSubdomain.get(1);
                    System.out.println("publicationCount:" + rankSubdomain.get("publicationCount") + " - citationCount:" + rankSubdomain.get("citationCount") + " - coAuthorCount:" + rankSubdomain.get("coAuthorCount") + " - h_index:" + rankSubdomain.get("h_index") + " - g_index:" + rankSubdomain.get("g_index"));
               // }
            }
            searcher.close();
            directory.close();

        } catch (Exception ex) {
            System.out.println("error: " + ex.toString());
        } finally {
        }
    }

    public static BooleanQuery searchIndex(String field, String phrase) throws IOException, ParseException {
        BooleanQuery booleanQuery = new BooleanQuery();
        QueryParser parser = new QueryParser(Version.LUCENE_36, field, new StandardAnalyzer(Version.LUCENE_36));
        Query query = parser.parse(phrase);
        booleanQuery.add(query, Occur.MUST);
        return booleanQuery;
    }

    public static BooleanQuery searchIndexWithMultiField(String[] fields, String phrase) throws IOException, ParseException {
        BooleanQuery booleanQuery = new BooleanQuery();
        MultiFieldQueryParser parserMultiField = new MultiFieldQueryParser(Version.LUCENE_34, fields, new StandardAnalyzer(Version.LUCENE_34));
        Query query = parserMultiField.parse(phrase);
        booleanQuery.add(query, Occur.MUST);
        return booleanQuery;
    }

    /*
     *
     * Searches mails that contain the word "java" in subject field.
     */
    public static BooleanQuery searchIndexWithTermQuery(String field, String phrase) throws IOException, ParseException {
        BooleanQuery booleanQuery = new BooleanQuery();
        Term term = new Term(field, phrase);
        Query query = new TermQuery(term);
        booleanQuery.add(query, Occur.MUST);
        return booleanQuery;
    }

    /*
     * Searches mails that contain a give phrase in the subject field.
     */
    public static BooleanQuery searchIndexWithPhraseQuery(String field, String[] phrases) throws IOException, ParseException {
        BooleanQuery booleanQuery = new BooleanQuery();
        PhraseQuery query = new PhraseQuery();
        query.setSlop(1);
        //Add terms of the phrases.
        for (int i = 0; i < phrases.length; i++) {
            query.add(new Term(field, phrases[i]));
        }
        booleanQuery.add(query, Occur.MUST);
        return booleanQuery;
    }

    /**
     * Searches for emails that have word similar to 'admnistrtor' in the
     * subject field. Note that we have misspelled the word and looking for a
     * word that is a close match to this.
     */
    public static BooleanQuery searchIndexWithFuzzyQuery(String field, String phrase) throws IOException, ParseException {
        BooleanQuery booleanQuery = new BooleanQuery();
        Query query = new FuzzyQuery(new Term(field, phrase));
        booleanQuery.add(query, Occur.MUST);
        return booleanQuery;
    }

    /**
     * Searches mails having sender field prefixed by the word "job"
     */
    public static BooleanQuery searchIndexWithPrefixQuery(String field, String phrase) throws IOException, ParseException {
        BooleanQuery booleanQuery = new BooleanQuery();
        PrefixQuery query = new PrefixQuery(new Term(field, phrase));
        booleanQuery.add(query, Occur.MUST);
        return booleanQuery;
    }

    /**
     * Searches mails that contain both "java" and "bangalore" in the subject
     * field
     */
    public static BooleanQuery searchIndexWithBooleanQuery(String field, String[] phrases) throws IOException, ParseException {
        BooleanQuery booleanQuery = new BooleanQuery();
        for (int i = 0; i < phrases.length; i++) {
            Query query = new TermQuery(new Term(field, phrases[i]));
            booleanQuery.add(query, Occur.MUST);
        }
        return booleanQuery;
    }

    /**
     * Searches mails that have word 'architect' in subject field.
     * WildcardQuery: Search for 'arch*' to find emails that have word
     * 'architect' in subject field.
     */
    public static BooleanQuery searchIndexWithWildCardQuery(String field, String phrase) throws IOException, ParseException {
        BooleanQuery booleanQuery = new BooleanQuery();
        Query query = new WildcardQuery(new Term(field, phrase + "*"));
        booleanQuery.add(query, Occur.MUST);
        return booleanQuery;
    }
}