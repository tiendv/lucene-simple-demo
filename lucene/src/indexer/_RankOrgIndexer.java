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
import java.util.ArrayList;
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
public class _RankOrgIndexer {

    private ConnectionPool connectionPool;
    private IndexSearcher searcher = null;
    private String path = null;
    public Boolean connect = true;
    public Boolean folder = true;

    public _RankOrgIndexer(String username, String password, String database, int port, String path) {
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
            File indexDir = new File(path + IndexConst.RANK_ORG_INDEX_PATH);
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
                ArrayList<Integer> listIdOrg = indexBO.getListIdOrgFromIdSubDomain(path + IndexConst.ORG_INDEX_PATH, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID));
                for (int i = 0; i < listIdOrg.size(); i++) {
                    int pubLast5Year = 0;
                    int citLast5Year = 0;
                    int g_indexLast5Year = 0;
                    int h_indexLast5Year = 0;
                    int pubLast10Year = 0;
                    int citLast10Year = 0;
                    int g_indexLast10Year = 0;
                    int h_indexLast10Year = 0;
                    int publicationCount = 0;
                    int citationCount = 0;
                    int h_index = 0;
                    int g_index = 0;
                    LinkedHashMap<String, Object> objectAllYear = indexBO.getPapersForRankSubDomain(path + IndexConst.PAPER_INDEX_PATH, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), Integer.toString(listIdOrg.get(i)), 0, 4);
                    if (objectAllYear != null) {
                        ArrayList<Integer> publicationListAllYear = (ArrayList<Integer>) objectAllYear.get("list");
                        LinkedHashMap<String, Integer> indexAllYear = indexBO.getCalculateIndex(publicationListAllYear);
                        publicationCount = Integer.parseInt(objectAllYear.get("pubCount").toString());
                        citationCount = Integer.parseInt(objectAllYear.get("citCount").toString());
                        h_index = indexAllYear.get("h_index");
                        g_index = indexAllYear.get("g_index");
                        LinkedHashMap<String, Object> object10Year = indexBO.getPapersForRankSubDomain(path + IndexConst.PAPER_INDEX_PATH, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), Integer.toString(listIdOrg.get(i)), 10, 4);
                        if (object10Year != null) {
                            ArrayList<Integer> publicationList10Year = (ArrayList<Integer>) object10Year.get("list");
                            LinkedHashMap<String, Integer> index10Year = indexBO.getCalculateIndex(publicationList10Year);
                            pubLast10Year = Integer.parseInt(object10Year.get("pubCount").toString());
                            citLast10Year = Integer.parseInt(object10Year.get("citCount").toString());
                            g_indexLast10Year = index10Year.get("g_index");
                            h_indexLast10Year = index10Year.get("h_index");
                            LinkedHashMap<String, Object> object5Year = indexBO.getPapersForRankSubDomain(path + IndexConst.PAPER_INDEX_PATH, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), Integer.toString(listIdOrg.get(i)), 5, 4);
                            if (object5Year != null) {
                                ArrayList<Integer> publicationList5Year = (ArrayList<Integer>) object5Year.get("list");
                                LinkedHashMap<String, Integer> index5Year = indexBO.getCalculateIndex(publicationList5Year);
                                pubLast5Year = Integer.parseInt(object5Year.get("pubCount").toString());
                                citLast5Year = Integer.parseInt(object5Year.get("citCount").toString());
                                g_indexLast5Year = index5Year.get("g_index");
                                h_indexLast5Year = index5Year.get("h_index");
                            }
                        }
                    }
                    Document d = new Document();
                    d.add(new NumericField(IndexConst.RANK_ORG_IDORG_FIELD, Field.Store.YES, false).setIntValue(listIdOrg.get(i)));
                    d.add(new Field(IndexConst.RANK_ORG_IDSUBDOMAIN_FIELD, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), Field.Store.YES, Field.Index.ANALYZED));
                    d.add(new NumericField(IndexConst.RANK_ORG_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                    d.add(new NumericField(IndexConst.RANK_ORG_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                    d.add(new NumericField(IndexConst.RANK_ORG_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                    d.add(new NumericField(IndexConst.RANK_ORG_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));
                    d.add(new NumericField(IndexConst.RANK_ORG_HINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast5Year));
                    d.add(new NumericField(IndexConst.RANK_ORG_HINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast10Year));
                    d.add(new NumericField(IndexConst.RANK_ORG_GINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast5Year));
                    d.add(new NumericField(IndexConst.RANK_ORG_GINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast10Year));
                    d.add(new NumericField(IndexConst.RANK_ORG_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(publicationCount));
                    d.add(new NumericField(IndexConst.RANK_ORG_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(citationCount));
                    d.add(new NumericField(IndexConst.RANK_ORG_HINDEX_FIELD, Field.Store.YES, true).setIntValue(h_index));
                    d.add(new NumericField(IndexConst.RANK_ORG_GINDEX_FIELD, Field.Store.YES, true).setIntValue(g_index));
                    writer.addDocument(d);
                    System.out.println("Indexing : " + count++ + "\t idOrg:" + listIdOrg.get(i) + "\t idSubdomain:" + rs.getString(SubdomainTB.COLUMN_SUBDOMAINID) + "\t pubLast5Year:" + pubLast5Year + "\t citLast5Year:" + citLast5Year + "\t pubLast10Year:" + pubLast10Year + "\t citLast10Year:" + citLast10Year);
                    d = null;
                }
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
            _RankOrgIndexer indexer = new _RankOrgIndexer(user, pass, database, port, path);
            System.out.println(indexer._run());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}