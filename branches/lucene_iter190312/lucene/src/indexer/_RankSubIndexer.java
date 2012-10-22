/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import bo.IndexBO;
import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import database.SubdomainTB;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.LinkedHashMap;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class _RankSubIndexer {

    private ConnectionPool connectionPool;
    private IndexSearcher searcher = null;
    private String path = null;
    public Boolean connect = true;
    public Boolean folder = true;

    public _RankSubIndexer(String username, String password, String database, int port, String path) {
        try {
            FSDirectory directory = Common.getFSDirectory(path, IndexConst.PAPER_INDEX_PATH);
            if (directory == null) {
                folder = false;
            }
            this.path = path;
            searcher = new IndexSearcher(directory);
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
            File indexDir = new File(path + IndexConst.RANK_SUBDOMAIN_INDEX_PATH);
            long start = new Date().getTime();
            int count = this._index(indexDir);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }

    public int _index(File indexDir) {
        int count = 0;
        IndexBO indexBO = new IndexBO();
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            // Connection to DB
            Connection connection = connectionPool.getConnection();
            String sql = "SELECT * FROM " + SubdomainTB.TABLE_NAME + " s";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            while ((rs != null) && (rs.next())) {
                int pubLast5Year = 0;
                int citLast5Year = 0;
                int pubLast10Year = 0;
                int citLast10Year = 0;
                int publicationCount = 0;
                int citationCount = 0;
                LinkedHashMap<String, Integer> objectAllYear = indexBO.getPublicationsFromIdSubdomain(path + IndexConst.PAPER_INDEX_PATH, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), 0);
                if (objectAllYear != null) {
                    publicationCount = objectAllYear.get("pubCount");
                    citationCount = objectAllYear.get("citCount");
                    LinkedHashMap<String, Integer> object10Year = indexBO.getPublicationsFromIdSubdomain(path + IndexConst.PAPER_INDEX_PATH, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), 10);
                    if (object10Year != null) {
                        pubLast10Year = object10Year.get("pubCount");
                        citLast10Year = object10Year.get("citCount");
                        LinkedHashMap<String, Integer> object5Year = indexBO.getPublicationsFromIdSubdomain(path + IndexConst.PAPER_INDEX_PATH, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), 5);
                        pubLast5Year = object5Year.get("pubCount");
                        citLast5Year = object5Year.get("citCount");
                    }
                }
                Document d = new Document();
                d.add(new Field(IndexConst.RANK_SUBDOMAIN_IDSUBDOMAIN_FIELD, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), Field.Store.YES, Field.Index.ANALYZED));
                d.add(new NumericField(IndexConst.RANK_SUBDOMAIN_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                d.add(new NumericField(IndexConst.RANK_SUBDOMAIN_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                d.add(new NumericField(IndexConst.RANK_SUBDOMAIN_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                d.add(new NumericField(IndexConst.RANK_SUBDOMAIN_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));
                d.add(new NumericField(IndexConst.RANK_SUBDOMAIN_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(publicationCount));
                d.add(new NumericField(IndexConst.RANK_SUBDOMAIN_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(citationCount));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t idSubdomain:" + rs.getString(SubdomainTB.COLUMN_SUBDOMAINID) + "\t pubLast5Year:" + pubLast5Year + "\t citLast5Year:" + citLast5Year + "\t pubLast10Year:" + pubLast10Year + "\t citLast10Year:" + citLast10Year);
                d = null;
            }
            count = writer.numDocs();
            writer.optimize();
            writer.close();
            stmt.close();
            connection.close();
            connectionPool.getConnection().close();
            connectionPool = null;
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
            _RankSubIndexer indexer = new _RankSubIndexer(user, pass, database, port, path);
            System.out.println(indexer._run());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}