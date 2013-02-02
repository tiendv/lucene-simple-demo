/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import database.SubdomainTB;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import searcher.ConferenceSearcher;
import searcher.PaperSearcher;

/**
 *
 * @author HuyDang
 */
public class _RankConfIndexer {

    private String path = "E:\\INDEX\\";

    /**
     *
     * @param path
     * @Summary: khởi tạo connection và searcher bằng các thông số trên
     */
    public _RankConfIndexer(String path) {
        this.path = path;
    }

    /**
     * @Summary Khởi chạy index
     * @return chuỗi thông báo số doc được index và thời gian index
     */
    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.RANK_CONFERENCE_INDEX_PATH);
            long start = new Date().getTime();
            int count = this._index(connectionPool, indexDir);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }

    /**
     * Thực hiện truy vấn thông tin IdConference và IdSubdomain. Đối với mỗi hội
     * nghị thực hiện truy vấn và tính toán các chỉ số: PublicationCount,
     * citationCount, H-Index, G-index trong 5 năm và 10 năm gần đây
     *
     * @param indexDir
     * @return
     */
    public int _index(ConnectionPool connectionPool, File indexDir) throws IOException, SQLException {
        int count = 0;
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
        Directory directory = FSDirectory.open(indexDir);
        IndexWriter writer = new IndexWriter(directory, config);
        // Connection to DB
        Connection connection = connectionPool.getConnection();
        try {
            String sql = "SELECT * FROM " + SubdomainTB.TABLE_NAME + " s";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            PaperSearcher paperSearcher = new PaperSearcher();
            ConferenceSearcher conferenceSearcher = new ConferenceSearcher();
            while ((rs != null) && (rs.next())) {
                ArrayList<Integer> listIdConference = conferenceSearcher.getListIdConferenceFromIdSubDomain(path, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID));
                for (int i = 0; i < listIdConference.size(); i++) {
                    LinkedHashMap<String, Object> objectAllYear = paperSearcher.getPapersForRankSubDomain(path, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), Integer.toString(listIdConference.get(i)), 0, 2);
                    if (!objectAllYear.isEmpty()) {
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

                        ArrayList<Integer> publicationListAllYear = (ArrayList<Integer>) objectAllYear.get("list");
                        LinkedHashMap<String, Integer> indexAllYear = Common.getCalculateIndex(publicationListAllYear);
                        publicationCount = Integer.parseInt(objectAllYear.get("pubCount").toString());
                        citationCount = Integer.parseInt(objectAllYear.get("citCount").toString());
                        h_index = indexAllYear.get("h_index");
                        g_index = indexAllYear.get("g_index");
                        LinkedHashMap<String, Object> object10Year = paperSearcher.getPapersForRankSubDomain(path, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), Integer.toString(listIdConference.get(i)), 10, 2);
                        if (!object10Year.isEmpty()) {
                            ArrayList<Integer> publicationList10Year = (ArrayList<Integer>) object10Year.get("list");
                            LinkedHashMap<String, Integer> index10Year = Common.getCalculateIndex(publicationList10Year);
                            pubLast10Year = Integer.parseInt(object10Year.get("pubCount").toString());
                            citLast10Year = Integer.parseInt(object10Year.get("citCount").toString());
                            g_indexLast10Year = index10Year.get("g_index");
                            h_indexLast10Year = index10Year.get("h_index");
                            LinkedHashMap<String, Object> object5Year = paperSearcher.getPapersForRankSubDomain(path, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), Integer.toString(listIdConference.get(i)), 5, 2);
                            if (!object5Year.isEmpty()) {
                                ArrayList<Integer> publicationList5Year = (ArrayList<Integer>) object5Year.get("list");
                                LinkedHashMap<String, Integer> index5Year = Common.getCalculateIndex(publicationList5Year);
                                pubLast5Year = Integer.parseInt(object5Year.get("pubCount").toString());
                                citLast5Year = Integer.parseInt(object5Year.get("citCount").toString());
                                g_indexLast5Year = index5Year.get("g_index");
                                h_indexLast5Year = index5Year.get("h_index");
                            }
                        }

                        Document d = new Document();
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_IDCONFERENCE_FIELD, Field.Store.YES, false).setIntValue(listIdConference.get(i)));
                        d.add(new Field(IndexConst.RANK_CONFERENCE_IDSUBDOMAIN_FIELD, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), Field.Store.YES, Field.Index.ANALYZED));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_HINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast5Year));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_HINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast10Year));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_GINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast5Year));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_GINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast10Year));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(publicationCount));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(citationCount));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_HINDEX_FIELD, Field.Store.YES, true).setIntValue(h_index));
                        d.add(new NumericField(IndexConst.RANK_CONFERENCE_GINDEX_FIELD, Field.Store.YES, true).setIntValue(g_index));
                        writer.addDocument(d);
                        System.out.println("Indexing : " + count++ + "\t idConference:" + listIdConference.get(i) + "\t idSubdomain:" + rs.getString(SubdomainTB.COLUMN_SUBDOMAINID) + "\t pubLast5Year:" + pubLast5Year + "\t citLast5Year:" + citLast5Year + "\t pubLast10Year:" + pubLast10Year + "\t citLast10Year:" + citLast10Year);
                        d = null;
                    }
                }
            }
            rs.close();
            stmt.close();
            count = writer.numDocs();
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
     * hàm để Test index
     *
     * @param args
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
            _RankConfIndexer indexer = new _RankConfIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}