/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import database.AuthorPaperTB;
import database.AuthorTB;
import database.PaperTB;
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
public class AuthorConfIndexer {

    private String path = "E:\\";

    public AuthorConfIndexer(String path) {
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
            Directory directory = FSDirectory.open(new File(path + "INDEX-AUTHOR-CONFERENCE"));
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
                Connection confCon = connectionPool.getConnection();
                String confQuery = "SELECT a." + AuthorPaperTB.COLUMN_AUTHORID + ", p." + PaperTB.COLUMN_CONFERENCEID + ", COUNT(*) AS publicationCount FROM " + AuthorPaperTB.TABLE_NAME + " a JOIN " + PaperTB.TABLE_NAME + " p ON (a." + AuthorPaperTB.COLUMN_PAPERID + " = p." + PaperTB.COLUMN_PAPERID + ") WHERE p." + PaperTB.COLUMN_CONFERENCEID + " IS NOT NULL AND a." + AuthorPaperTB.COLUMN_AUTHORID + "=" + authRs.getString(AuthorTB.COLUMN_AUTHORID) + " GROUP BY a." + AuthorPaperTB.COLUMN_AUTHORID + ", p." + PaperTB.COLUMN_CONFERENCEID;
                PreparedStatement confStmt = confCon.prepareStatement(confQuery);
                ResultSet confRs = confStmt.executeQuery();
                while ((confRs != null) && (confRs.next())) {
                    Document d = new Document();
                    d.add(new Field("idAuthor", authRs.getString(AuthorTB.COLUMN_AUTHORID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("idConference", confRs.getString(PaperTB.COLUMN_CONFERENCEID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new NumericField("publicationCount", Field.Store.YES, true).setIntValue(confRs.getInt("publicationCount")));
                    writer.addDocument(d);
                    d = null;
                    System.out.println("Indexing: " + count++ + "\t" + " idAuthor: " + authRs.getString(AuthorTB.COLUMN_AUTHORID) + "\t" + " idConference: " + confRs.getString(PaperTB.COLUMN_CONFERENCEID) + "\t" + " publicationCount: " + confRs.getString("publicationCount"));
                }
                confRs.close();
                confStmt.close();
                confCon.close();
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
            String database = "pubguru";
            int port = 3306;
            String path = "E:\\INDEX\\";
            ConnectionPool connectionPool = new ConnectionPool(user, pass, database, port);
            AuthorConfIndexer indexer = new AuthorConfIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}