/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package searcher;

import constant.Common;
import constant.IndexConst;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
public class AuthorSearcher {

    private static IndexSearcher searcher = null;

    public static IndexSearcher getSearcher(String path) {
        try {
            if (searcher == null) {
                searcher = new IndexSearcher(Common.getFSDirectory(path, IndexConst.AUTHOR_INDEX_PATH));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return searcher;
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @return listIdAuthor
     * @throws Exception
     */
    public ArrayList<Object> getListIdAuthorFromIdSubDomain(String path, String idSubdomain) throws Exception {
        ArrayList<Object> out = new ArrayList<Object>();
        try {
            BooleanQuery blQuery = new BooleanQuery();
            QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.AUTHOR_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query query = parser.parse(idSubdomain);
            blQuery.add(query, BooleanClause.Occur.MUST);
            TopDocs topDocs = getSearcher(path).search(blQuery, Integer.MAX_VALUE);
            if (topDocs != null && topDocs.totalHits > 0) {
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = getSearcher(path).doc(hit.doc);
                    LinkedHashMap<String, String> item = new LinkedHashMap<String, String>();
                    item.put("idAuthor", doc.get(IndexConst.AUTHOR_IDAUTHOR_FIELD));
                    item.put("idOrg", doc.get(IndexConst.AUTHOR_IDORG_FIELD));
                    out.add(item);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
        }
        return out;
    }
}