/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import constant.IndexConst;
import database.ConferenceTB;
import database.KeywordTB;
import database.PaperKeywordTB;
import database.PaperTB;
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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class ConferenceKeyIndexer {

    private String path = "E:\\INDEX\\";

    public ConferenceKeyIndexer(String path) {
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
     * Truy vấn các thông tin conference, keyword, publicationCount từ csdl thực
     * hiện index
     *
     * @param connectionPool: kết nối csdl
     * @param indexDir: thư mục lưu trữ file index
     * @return số doc thực hiện
     */
    private int _index(ConnectionPool connectionPool) throws CorruptIndexException, LockObtainFailedException, IOException, SQLException {
        int count = 0;

        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
        Directory directory = FSDirectory.open(new File(path + IndexConst.CONFERENCE_KEYWORD_DIRECTORY_PATH));
        IndexWriter writer = new IndexWriter(directory, config);
        // Connection to DB
        Connection connection = connectionPool.getConnection();
        try {
            String confQuery = "SELECT " + ConferenceTB.COLUMN_CONFERENCEID + ", " + ConferenceTB.COLUMN_CONFERENCENAME + " FROM " + ConferenceTB.TABLE_NAME + " a";
            PreparedStatement confStmt = connection.prepareStatement(confQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            confStmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet confRs = confStmt.executeQuery();
            // Index data from query
            while ((confRs != null) && (confRs.next())) {
                // Connection to DB
                Connection keyCon = connectionPool.getConnection();
                String keyQuery = "SELECT pk." + PaperKeywordTB.COLUMN_KEYWORDID + ", (SELECT k." + KeywordTB.COLUMN_KEYWORD + " FROM " + KeywordTB.TABLE_NAME + " k WHERE k." + KeywordTB.COLUMN_KEYWORDID + " = pk." + PaperKeywordTB.COLUMN_KEYWORDID + ") AS keyword, COUNT(*) AS publicationCount FROM " + PaperTB.TABLE_NAME + " p JOIN " + PaperKeywordTB.TABLE_NAME + " pk ON (p." + PaperTB.COLUMN_PAPERID + " = pk." + PaperKeywordTB.COLUMN_PAPERID + ") WHERE p." + PaperTB.COLUMN_CONFERENCEID + "=" + confRs.getString(ConferenceTB.COLUMN_CONFERENCEID) + " GROUP BY p." + PaperTB.COLUMN_CONFERENCEID + ", pk." + PaperKeywordTB.COLUMN_KEYWORDID;
                PreparedStatement keyStmt = keyCon.prepareStatement(keyQuery);
                ResultSet keyRs = keyStmt.executeQuery();
                while ((keyRs != null) && (keyRs.next())) {
                    Document d = new Document();
                    d.add(new Field("idConference", confRs.getString(ConferenceTB.COLUMN_CONFERENCEID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("conferenceName", confRs.getString(ConferenceTB.COLUMN_CONFERENCENAME), Field.Store.YES, Field.Index.NO));
                    d.add(new Field("idKeyword", keyRs.getString(PaperKeywordTB.COLUMN_KEYWORDID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("keyword", keyRs.getString("keyword"), Field.Store.YES, Field.Index.NO));
                    d.add(new NumericField("publicationCount", Field.Store.YES, true).setIntValue(keyRs.getInt("publicationCount")));
                    writer.addDocument(d);
                    d = null;
                    System.out.println("Indexing: " + count++ + "\t" + " idConference: " + confRs.getString(ConferenceTB.COLUMN_CONFERENCEID) + "\t" + " conferenceName: " + confRs.getString(ConferenceTB.COLUMN_CONFERENCENAME) + "\t" + " idKeyword: " + keyRs.getString(PaperKeywordTB.COLUMN_KEYWORDID) + "\t" + " keyword: " + keyRs.getString("keyword") + "\t" + " publicationCount: " + keyRs.getString("publicationCount"));
                }
                keyRs.close();
                keyStmt.close();
                keyCon.close();
            }
            confStmt.close();
        } catch (Exception ex) {
            ex.printStackTrace();
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
            ConferenceKeyIndexer indexer = new ConferenceKeyIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}