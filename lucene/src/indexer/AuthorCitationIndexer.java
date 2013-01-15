/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import constant.IndexConst;
import database.AuthorPaperTB;
import database.AuthorTB;
import database.PaperPaperTB;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.json.simple.JSONObject;

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

    /**
     * Truy vấn các thông tin authorCitation từ csdl thực hiện index
     *
     * @param connectionPool: kết nối csdl
     * @param indexDir: thư mục lưu trữ file index
     * @return số doc thực hiện
     */
    private int _index(ConnectionPool connectionPool) throws IOException, SQLException {
        int count = 0;
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
        Directory directory = FSDirectory.open(new File(path + IndexConst.AUTHOR_CITATION_DIRECTORY_PATH));
        IndexWriter writer = new IndexWriter(directory, config);
        try {
            for (int i = 1; i <= this.getMaxIdAuthor(connectionPool); i++) {
                String cAuthors = this.cAuthors(connectionPool, i);
                if (cAuthors == null) {
                    continue;
                }
                Document d = new Document();
                d.add(new Field("idAuthor", Integer.toString(i), Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field("cAuthors", cAuthors, Field.Store.YES, Field.Index.NO));
                writer.addDocument(d);
                d = null;
                System.out.println("Indexing: " + count++ + "\t" + " idAuthor: " + Integer.toString(i));
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            writer.optimize();
            writer.close();
            directory.close();
        }
        return count;
    }

    /**
     * Lấy idAuthor lớn nhất trong database
     *
     * @author HuyDang
     * @param connectionPool
     * @return
     * @throws SQLException
     */
    private int getMaxIdAuthor(ConnectionPool connectionPool) throws SQLException {
        int id = 0;
        // Connection to DB
        Connection connection = connectionPool.getConnection();
        try {
            String sql = "SELECT " + AuthorTB.COLUMN_AUTHORID + " FROM " + AuthorTB.TABLE_NAME + " a ORDER BY " + AuthorTB.COLUMN_AUTHORID + " DESC LIMIT 1";
            PreparedStatement stmt = connection.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            if ((rs != null) && (rs.next())) {
                id = rs.getInt(AuthorTB.COLUMN_AUTHORID);
            }
            rs.close();
            stmt.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            connection.close();
        }
        return id;
    }

    /**
     * Lấy danh sách các tác giả mà idAuthor tham khảo
     *
     * @author HuyDang
     * @param connectionPool
     * @param idAuthor
     * @return
     * @throws SQLException
     */
    private String cAuthors(ConnectionPool connectionPool, int idAuthor) throws SQLException {
        ArrayList<Object> cAuthors = new ArrayList<Object>();
        Map json = new HashMap();
        Connection connection = connectionPool.getConnection();
        try {
            String list = this.getListIdPaperFromAuthor(connectionPool, idAuthor);
            if (!"".equals(list.trim())) {
                String sql = "SELECT ap." + AuthorPaperTB.COLUMN_AUTHORID + ", COUNT(ap." + AuthorPaperTB.COLUMN_AUTHORID + ") AS citationCount "
                        + "FROM " + PaperPaperTB.TABLE_NAME + " pp JOIN " + AuthorPaperTB.TABLE_NAME + " ap ON (ap." + AuthorPaperTB.COLUMN_PAPERID + " = pp." + PaperPaperTB.COLUMN_PAPERID + ") "
                        + "WHERE pp." + PaperPaperTB.COLUMN_PAPERREFID + " IN (" + list + ") AND ap." + AuthorPaperTB.COLUMN_AUTHORID + " <> ? "
                        + "GROUP BY ap." + AuthorPaperTB.COLUMN_AUTHORID + " ORDER BY citationCount DESC LIMIT 30";
                PreparedStatement stmt = connection.prepareStatement(sql);
                stmt.setInt(1, idAuthor);
                ResultSet rs = stmt.executeQuery();
                while ((rs != null) && (rs.next())) {
                    LinkedHashMap<String, Integer> item = new LinkedHashMap<String, Integer>();
                    item.put("idAuthor", rs.getInt(AuthorPaperTB.COLUMN_AUTHORID));
                    item.put("citationCount", rs.getInt("citationCount"));
                    cAuthors.add(item);
                }
                rs.close();
                stmt.close();
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            connection.close();
        }
        if (cAuthors.size() > 0) {
            json.put("cAuthors", cAuthors);
            JSONObject outJSON = new JSONObject(json);
            return outJSON.toJSONString();
        }
        return null;
    }

    /**
     * Lấy danh sách idPaper của 1 tác giả
     *
     * @author HuyDang
     * @param connectionPool
     * @param idAuthor
     * @return
     * @throws SQLException
     */
    private String getListIdPaperFromAuthor(ConnectionPool connectionPool, int idAuthor) throws SQLException {
        String list = "";
        Connection connection = connectionPool.getConnection();
        try {
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
            rs.close();
            stmt.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            connection.close();
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
            String pass = "root";
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