/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import database.AuthorPaperTB;
import database.AuthorTB;
import database.PaperKeywordTB;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class AuthorKeyIndexer {

    private String path = "E:\\";

    public AuthorKeyIndexer(String path) {
        this.path = path;
    }

    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            long start = new Date().getTime();
            int count = this._index(connectionPool);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }

    private int _index(ConnectionPool connectionPool) {
        int count = 0;
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
            Directory directory = FSDirectory.open(new File(path + "INDEX-AUTHOR-KEYWORD"));
            IndexWriter writer = new IndexWriter(directory, config);
            // Connection to DB
            Connection connection = connectionPool.getConnection();
            String authQuery = "SELECT " + AuthorTB.COLUMN_AUTHORID + " FROM " + AuthorTB.TABLE_NAME + " a";
            PreparedStatement authorStmt = connection.prepareStatement(authQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            authorStmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet authRs = authorStmt.executeQuery();
            // Index data from query
            while ((authRs != null) && (authRs.next())) {
                // Connection to DB
                Connection keyCon = connectionPool.getConnection();
                String keyQuery = "SELECT  a." + AuthorPaperTB.COLUMN_AUTHORID + ", pk." + PaperKeywordTB.COLUMN_KEYWORDID + ", COUNT(*) AS publicationCount FROM " + AuthorPaperTB.TABLE_NAME + " a JOIN " + PaperKeywordTB.TABLE_NAME + " pk ON (pk." + PaperKeywordTB.COLUMN_PAPERID + " = a." + AuthorPaperTB.COLUMN_PAPERID + ") WHERE a." + AuthorPaperTB.COLUMN_AUTHORID + "=" + authRs.getString(AuthorTB.COLUMN_AUTHORID) + " GROUP BY a." + AuthorPaperTB.COLUMN_AUTHORID + ", pk." + PaperKeywordTB.COLUMN_KEYWORDID;
                PreparedStatement keyStmt = keyCon.prepareStatement(keyQuery);
                ResultSet keyRs = keyStmt.executeQuery();
                while ((keyRs != null) && (keyRs.next())) {
                    Document d = new Document();
                    d.add(new Field("idAuthor", authRs.getString(AuthorTB.COLUMN_AUTHORID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("idKeyword", keyRs.getString(PaperKeywordTB.COLUMN_KEYWORDID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new NumericField("publicationCount", Field.Store.YES, true).setIntValue(keyRs.getInt("publicationCount")));
                    writer.addDocument(d);
                    d = null;
                    System.out.println("Indexing: " + count++ + "\t" + " idAuthor: " + authRs.getString(AuthorTB.COLUMN_AUTHORID) + "\t" + " idKeyword: " + keyRs.getString(PaperKeywordTB.COLUMN_KEYWORDID) + "\t" + " publicationCount: " + keyRs.getString("publicationCount"));
                }
                keyRs.close();
                keyStmt.close();
                keyCon.close();
            }
            authorStmt.close();
            connection.close();
            writer.optimize();
            writer.close();
            directory.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return 0;
        }
        return count;
    }

    /**
     *
     * @Summary: ham test index
     */
    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String user = "root";
            String pass = "@huydang1920@";
            String database = "cspublicationcrawler";
            int port = 3306;
            String path = "E:\\GURU\\";
            ConnectionPool connectionPool = new ConnectionPool(user, pass, database, port);
            AuthorKeyIndexer indexer = new AuthorKeyIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}