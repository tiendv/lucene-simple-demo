/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import constant.IndexConst;
import database.JournalTB;
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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class JournalKeyIndexer {

    private String path = "E:\\";

    public JournalKeyIndexer(String path) {
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
     * Truy vấn các thông tin journal, keyword, publicationCount từ csdl thực
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
        Directory directory = FSDirectory.open(new File(path + IndexConst.JOURNAL_KEYWORD_DIRECTORY_PATH));
        IndexWriter writer = new IndexWriter(directory, config);
        // Connection to DB
        Connection connection = connectionPool.getConnection();
        try {
            String jourQuery = "SELECT " + JournalTB.COLUMN_JOURNALID + ", " + JournalTB.COLUMN_JOURNALNAME + " FROM " + JournalTB.TABLE_NAME + " a";
            PreparedStatement jourStmt = connection.prepareStatement(jourQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            jourStmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet jourRs = jourStmt.executeQuery();
            // Index data from query
            while ((jourRs != null) && (jourRs.next())) {
                // Connection to DB
                Connection keyCon = connectionPool.getConnection();
                String keyQuery = "SELECT pk." + PaperKeywordTB.COLUMN_KEYWORDID + ", (SELECT k." + KeywordTB.COLUMN_KEYWORD + " FROM " + KeywordTB.TABLE_NAME + " k WHERE k." + KeywordTB.COLUMN_KEYWORDID + " = pk." + PaperKeywordTB.COLUMN_KEYWORDID + ") AS keyword, COUNT(*) AS publicationCount FROM " + PaperTB.TABLE_NAME + " p JOIN " + PaperKeywordTB.TABLE_NAME + " pk ON (p." + PaperTB.COLUMN_PAPERID + " = pk." + PaperKeywordTB.COLUMN_PAPERID + ") WHERE p." + PaperTB.COLUMN_JOURNALID + "=" + jourRs.getString(JournalTB.COLUMN_JOURNALID) + " GROUP BY p." + PaperTB.COLUMN_JOURNALID + ", pk." + PaperKeywordTB.COLUMN_KEYWORDID;
                PreparedStatement keyStmt = keyCon.prepareStatement(keyQuery);
                ResultSet keyRs = keyStmt.executeQuery();
                while ((keyRs != null) && (keyRs.next())) {
                    Document d = new Document();
                    d.add(new Field("idJournal", jourRs.getString(JournalTB.COLUMN_JOURNALID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("journalName", jourRs.getString(JournalTB.COLUMN_JOURNALNAME), Field.Store.YES, Field.Index.NO));
                    d.add(new Field("idKeyword", keyRs.getString(PaperKeywordTB.COLUMN_KEYWORDID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("keyword", keyRs.getString("keyword"), Field.Store.YES, Field.Index.NO));
                    d.add(new NumericField("publicationCount", Field.Store.YES, true).setIntValue(keyRs.getInt("publicationCount")));
                    writer.addDocument(d);
                    d = null;
                    System.out.println("Indexing: " + count++ + "\t" + " idJournal: " + jourRs.getString(JournalTB.COLUMN_JOURNALID) + "\t" + " journalName: " + jourRs.getString(JournalTB.COLUMN_JOURNALNAME) + "\t" + " idKeyword: " + keyRs.getString(PaperKeywordTB.COLUMN_KEYWORDID) + "\t" + " keyword: " + keyRs.getString("keyword") + "\t" + " publicationCount: " + keyRs.getString("publicationCount"));
                }
                keyRs.close();
                keyStmt.close();
                keyCon.close();
            }
            jourStmt.close();
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
            JournalKeyIndexer indexer = new JournalKeyIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}