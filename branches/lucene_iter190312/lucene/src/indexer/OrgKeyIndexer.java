/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import database.AuthorPaperTB;
import database.AuthorTB;
import database.OrgTB;
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
public class OrgKeyIndexer {

    private String path = "E:\\";

    public OrgKeyIndexer(String path) {
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
            Directory directory = FSDirectory.open(new File(path + "INDEX-ORG-KEYWORD"));
            IndexWriter writer = new IndexWriter(directory, config);
            // Connection to DB
            Connection connection = connectionPool.getConnection();
            String orgQuery = "SELECT " + OrgTB.COLUMN_ORGID + " FROM " + OrgTB.TABLE_NAME + " a limit 1";
            PreparedStatement orgStmt = connection.prepareStatement(orgQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            orgStmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet orgRs = orgStmt.executeQuery();
            // Index data from query
            while ((orgRs != null) && (orgRs.next())) {
                // Connection to DB
                Connection keyCon = connectionPool.getConnection();
                String keyQuery = "SELECT a." + AuthorTB.COLUMN_ORGID + ", pk." + PaperKeywordTB.COLUMN_KEYWORDID + ", COUNT(DISTINCT pk." + PaperKeywordTB.COLUMN_PAPERID + ") AS publicationCount FROM " + AuthorTB.TABLE_NAME + " a JOIN " + AuthorPaperTB.TABLE_NAME + " ap ON (a." + AuthorTB.COLUMN_AUTHORID + " = ap." + AuthorPaperTB.COLUMN_AUTHORID + ") JOIN " + PaperKeywordTB.TABLE_NAME + " pk ON (pk." + PaperKeywordTB.COLUMN_PAPERID + " = ap." + AuthorPaperTB.COLUMN_PAPERID + ") WHERE a." + AuthorTB.COLUMN_ORGID + "=" + orgRs.getString(OrgTB.COLUMN_ORGID) + " GROUP BY a." + AuthorTB.COLUMN_ORGID + ", pk." + PaperKeywordTB.COLUMN_KEYWORDID;
                PreparedStatement keyStmt = keyCon.prepareStatement(keyQuery);
                ResultSet keyRs = keyStmt.executeQuery();
                while ((keyRs != null) && (keyRs.next())) {
                    Document d = new Document();
                    d.add(new Field("idOrg", orgRs.getString(OrgTB.COLUMN_ORGID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new Field("idKeyword", keyRs.getString(PaperKeywordTB.COLUMN_KEYWORDID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new NumericField("publicationCount", Field.Store.YES, true).setIntValue(keyRs.getInt("publicationCount")));
                    writer.addDocument(d);
                    d = null;
                    System.out.println("Indexing: " + count++ + "\t" + " idOrg: " + orgRs.getString(OrgTB.COLUMN_ORGID) + "\t" + " idKeyword: " + keyRs.getString(PaperKeywordTB.COLUMN_KEYWORDID) + "\t" + " publicationCount: " + keyRs.getString("publicationCount"));
                }
                keyRs.close();
                keyStmt.close();
                keyCon.close();
            }
            orgStmt.close();
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
            OrgKeyIndexer indexer = new OrgKeyIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}