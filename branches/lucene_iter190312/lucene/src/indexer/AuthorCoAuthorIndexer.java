/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import constant.IndexConst;
import database.AuthorPaperTB;
import database.AuthorTB;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
public class AuthorCoAuthorIndexer {

    private String path = "E:\\";

    public AuthorCoAuthorIndexer(String path) {
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

    /**
     * Truy vấn các thông tin author, coAuthor, publicationCount từ csdl thực
     * hiện index
     *
     * @param connectionPool: kết nối csdl
     * @param indexDir: thư mục lưu trữ file index
     * @return số doc thực hiện
     */
    private int _index(ConnectionPool connectionPool) throws IOException, SQLException {
        int count = 0;
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
        Directory directory = FSDirectory.open(new File(path + IndexConst.AUTHOR_COAUTHOR_DIRECTORY_PATH));
        IndexWriter writer = new IndexWriter(directory, config);
        // Connection to DB
        Connection connection = connectionPool.getConnection();
        try {
            String authQuery = "SELECT " + AuthorTB.COLUMN_AUTHORID + ", " + AuthorTB.COLUMN_AUTHORNAME + " FROM " + AuthorTB.TABLE_NAME + " a";
            PreparedStatement authorStmt = connection.prepareStatement(authQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            authorStmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet authRs = authorStmt.executeQuery();
            // Index data from query
            while ((authRs != null) && (authRs.next())) {
                // Connection to DB
                Connection coCon = connectionPool.getConnection();
                String coQuery = "SELECT ap2." + AuthorPaperTB.COLUMN_AUTHORID + " AS coAuthor, (SELECT a." + AuthorTB.COLUMN_AUTHORNAME + " FROM " + AuthorTB.TABLE_NAME + " a WHERE a." + AuthorTB.COLUMN_AUTHORID + " = ap2." + AuthorPaperTB.COLUMN_AUTHORID + ") AS coAuthorName, COUNT(*) AS publicationCount FROM " + AuthorPaperTB.TABLE_NAME + " ap1 JOIN " + AuthorPaperTB.TABLE_NAME + " ap2 ON (ap1." + AuthorPaperTB.COLUMN_PAPERID + "=ap2." + AuthorPaperTB.COLUMN_PAPERID + ") WHERE ap1." + AuthorPaperTB.COLUMN_AUTHORID + "=" + authRs.getString(AuthorTB.COLUMN_AUTHORID) + " AND ap2." + AuthorPaperTB.COLUMN_AUTHORID + "<>" + authRs.getString(AuthorTB.COLUMN_AUTHORID) + " GROUP BY ap1." + AuthorPaperTB.COLUMN_AUTHORID + ", ap2." + AuthorPaperTB.COLUMN_AUTHORID + " ORDER BY publicationCount DESC";
                PreparedStatement coStmt = coCon.prepareStatement(coQuery);
                ResultSet coRs = coStmt.executeQuery();
                while ((coRs != null) && (coRs.next())) {
                    Document d = new Document();
                    d.add(new Field("idAuthor", authRs.getString(AuthorTB.COLUMN_AUTHORID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("authorName", authRs.getString(AuthorTB.COLUMN_AUTHORNAME), Field.Store.YES, Field.Index.NO));
                    d.add(new Field("coAuthor", coRs.getString("coAuthor"), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("coAuthorName", coRs.getString("coAuthorName"), Field.Store.YES, Field.Index.NO));
                    d.add(new NumericField("publicationCount", Field.Store.YES, true).setIntValue(coRs.getInt("publicationCount")));
                    writer.addDocument(d);
                    d = null;
                    System.out.println("Indexing: " + count++ + "\t" + " idAuthor: " + authRs.getString(AuthorTB.COLUMN_AUTHORID) + "\t" + " authorName: " + authRs.getString(AuthorTB.COLUMN_AUTHORNAME) + "\t" + " coAuthor: " + coRs.getString("coAuthor") + "\t" + " coAuthorName: " + coRs.getString("coAuthorName") + "\t" + " publicationCount: " + coRs.getString("publicationCount"));
                }
                coRs.close();
                coStmt.close();
                coCon.close();
            }
            authorStmt.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            connection.close();
            writer.optimize();
            writer.close();
            directory.close();
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
            String pass = "root";
            String database = "pubguru";
            int port = 3306;
            String path = "E:\\INDEX\\";
            ConnectionPool connectionPool = new ConnectionPool(user, pass, database, port);
            AuthorCoAuthorIndexer indexer = new AuthorCoAuthorIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}