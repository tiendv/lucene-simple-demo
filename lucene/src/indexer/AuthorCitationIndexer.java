/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import database.AuthorPaperTB;
import database.AuthorTB;
import database.PaperPaperTB;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
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
public class AuthorCitationIndexer {

    private String path = "E:\\";

    public AuthorCitationIndexer(String path) {
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
            Directory directory = FSDirectory.open(new File(path + "INDEX-AUTHOR-CITATION"));
            IndexWriter writer = new IndexWriter(directory, config);
            // Connection to DB
            Connection connection = connectionPool.getConnection();
            String query = "SELECT " + AuthorTB.COLUMN_AUTHORID + " FROM " + AuthorTB.TABLE_NAME + " a";
            PreparedStatement stmt = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            while ((rs != null) && (rs.next())) {
                ArrayList<Object> object = this.getObjects(connectionPool, rs.getInt(AuthorTB.COLUMN_AUTHORID));
                if (object == null) {
                    continue;
                }
                //idAuthor - cidAuthor - citationCount
                for (int i = 0; i < object.size(); i++) {
                    LinkedHashMap<String, Object> item = (LinkedHashMap<String, Object>) object.get(i);
                    Document d = new Document();
                    d.add(new Field("idAuthor", rs.getString(AuthorTB.COLUMN_AUTHORID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("cidAuthor", item.get("idAuthor").toString(), Field.Store.YES, Field.Index.NO));
                    d.add(new NumericField("citationCount", Field.Store.YES, true).setIntValue(Integer.parseInt(item.get("citationCount").toString())));
                    writer.addDocument(d);
                    d = null;
                    System.out.println("Indexing: " + count++ + "\t" + " idAuthor: " + rs.getString(AuthorTB.COLUMN_AUTHORID) + "\t" + " cidAuthor: " + item.get("idAuthor").toString() + "\t" + " citationCount: " + Integer.parseInt(item.get("citationCount").toString()));
                }
            }
            stmt.close();
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

    private ArrayList<Object> getObjects(ConnectionPool connectionPool, int idAuthor) {
        try {
            Connection connection = connectionPool.getConnection();
            String list = this.getListIdPaperFromAuthor(connectionPool, idAuthor);
            if ("".equals(list)) {
                connection.close();
                return null;
            }
            String sql = "SELECT ap." + AuthorPaperTB.COLUMN_AUTHORID + ", COUNT(ap." + AuthorPaperTB.COLUMN_AUTHORID + ") AS citationCount "
                    + "FROM " + PaperPaperTB.TABLE_NAME + " pp JOIN " + AuthorPaperTB.TABLE_NAME + " ap ON (ap." + AuthorPaperTB.COLUMN_PAPERID + " = pp." + PaperPaperTB.COLUMN_PAPERID + ") "
                    + "WHERE pp." + PaperPaperTB.COLUMN_PAPERREFID + " IN (" + list + ") AND ap." + AuthorPaperTB.COLUMN_AUTHORID + " <> ? "
                    + "GROUP BY ap." + AuthorPaperTB.COLUMN_AUTHORID;
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idAuthor);
            ResultSet rs = stmt.executeQuery();
            ArrayList<Object> out = new ArrayList<Object>();
            while ((rs != null) && (rs.next())) {
                LinkedHashMap<String, Object> item = new LinkedHashMap<String, Object>();
                item.put("idAuthor", rs.getInt(AuthorPaperTB.COLUMN_AUTHORID));
                item.put("citationCount", rs.getInt("citationCount"));
                out.add(item);
            }
            stmt.close();
            connection.close();
            return out;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return null;
    }

    private String getListIdPaperFromAuthor(ConnectionPool connectionPool, int idAuthor) {
        String list = "";
        try {
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT ap." + AuthorPaperTB.COLUMN_PAPERID + " FROM " + AuthorPaperTB.TABLE_NAME + " ap WHERE ap." + AuthorPaperTB.COLUMN_AUTHORID + " = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idAuthor);
            ResultSet rs = stmt.executeQuery();
            while ((rs != null) && (rs.next())) {
                list += "," + rs.getString(AuthorPaperTB.COLUMN_PAPERID);
            }
            if (!"".equals(list)) {
                list = list.substring(1);
            }
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return list;
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
            AuthorCitationIndexer indexer = new AuthorCitationIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}