/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package searcher;

import constant.Common;
import constant.IndexConst;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class KeywordSearcher {

    private static IndexSearcher searcher = null;

    public static IndexSearcher getSearcher(String path) {
        try {
            if (searcher == null) {
                searcher = new IndexSearcher(Common.getFSDirectory(path, IndexConst.KEYWORD_INDEX_PATH));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return searcher;
    }

    public void destroy() throws IOException {
        searcher.close();
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @return listIdKeyword
     */
    public ArrayList<Integer> getListIdKeywordFromIdSubDomain(String path, String idSubdomain) throws Exception {
        ArrayList<Integer> list = new ArrayList<Integer>();
        try {
            BooleanQuery blQuery = new BooleanQuery();
            QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.KEYWORD_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query query = parser.parse(idSubdomain);
            blQuery.add(query, BooleanClause.Occur.MUST);
            TopDocs topDocs = getSearcher(path).search(blQuery, Integer.MAX_VALUE);
            if (topDocs != null && topDocs.totalHits > 0) {
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = getSearcher(path).doc(hit.doc);
                    list.add(Integer.parseInt(doc.get(IndexConst.KEYWORD_IDKEYWORD_FIELD)));
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        return list;
    }
}