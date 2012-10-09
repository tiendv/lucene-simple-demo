/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import database.CcidfTB;
import dto.CcidfDTO;
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
public class CcidfIndexer {

    private ConnectionPool connectionPool;
    private String path = null;
    public Boolean connect = true;
    public Boolean folder = true;

    public CcidfIndexer(String username, String password, String database, int port, String path) {
        try {
            FSDirectory directory = Common.getFSDirectory(path, IndexConst.PAPER_INDEX_PATH);
            if (directory == null) {
                folder = false;
            }
            this.path = path;
            connectionPool = new ConnectionPool(username, password, database, port);
            if (connectionPool.getConnection() == null) {
                connect = false;
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    public String _run() {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.CCIDF_INDEX_PATH);
            long start = new Date().getTime();
            int count = this._index(indexDir, connectionPool);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }

    public int _index(File indexDir, ConnectionPool connectionPool) {
        int count = 0;
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            // Connection to DB
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT * FROM " + CcidfTB.TABLE_NAME + " c";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            CcidfDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new CcidfDTO();
                Document d = new Document();
                dto.setIdPaper(rs.getString(CcidfTB.COLUMN_PAPERID));
                dto.setIdRelatedPaper(rs.getInt(CcidfTB.COLUMN_RELATEDPAPERID));
                dto.setWeight(rs.getDouble(CcidfTB.COLUMN_WEIGHT));

                d.add(new Field(IndexConst.CCIDF_IDPAPER_FIELD, dto.idPaper, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new NumericField(IndexConst.CCIDF_IDRELATEPAPER_FIELD, Field.Store.YES, false).setIntValue(dto.idRelatedPaper));
                d.add(new NumericField(IndexConst.CCIDF_WEIGHT_FIELD, Field.Store.YES, false).setDoubleValue(dto.weight));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t");
                d = null;
                dto = null;
            }
            count = writer.numDocs();
            writer.optimize();
            writer.close();
            stmt.close();
            connection.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return 0;
        }
        return count;
    }

    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String user = "root";
            String pass = "@huydang1920@";
            String database = "cspublicationcrawler";
            int port = 3306;
            String path = "E:\\";
            CcidfIndexer indexer = new CcidfIndexer(user, pass, database, port, path);
            indexer._run();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}