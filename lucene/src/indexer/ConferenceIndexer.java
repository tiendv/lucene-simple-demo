/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import database.ConferenceTB;
import database.PaperTB;
import database.SubdomainPaperTB;
import dto.ConferenceDTO;
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
import searcher.PaperSearcher;

/**
 *
 * @author HuyDang
 */
public class ConferenceIndexer {

    private String path = "E:\\INDEX\\";

    /**
     * hàm khởi tạo searcher
     *
     * @param path : đường dẫn tới thư mục lưu trữ Index
     */
    public ConferenceIndexer(String path) {
        this.path = path;
    }

    /**
     * Hàm khởi chạy index
     *
     * @param connectionPool: kết nối csdl
     * @return số lượng doc thực hiện index và thời gian thực hiện index
     */
    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            File indexDir = new File(this.path + IndexConst.CONFERENCE_INDEX_PATH);
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
     * @Summary: truy vấn từ trong cơ sở dữ liệu ra tất cả các thông tin của một
     * conference. Lấy idConf đó thực hiện truy vấn và tính toán các chỉ số:
     * publicationCount, citationCount, H-index, G-index,..
     * @param connectionPool: kết nối tới csdl
     * @param indexDir : thư mục lưu trữ file index
     */
    private int _index(ConnectionPool connectionPool, File indexDir) throws IOException, SQLException {
        int count = 0;
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
        Directory directory = FSDirectory.open(indexDir);
        IndexWriter writer = new IndexWriter(directory, config);
        // Connection to DB
        Connection connection = connectionPool.getConnection();
        try {
            String sql = "SELECT * FROM " + ConferenceTB.TABLE_NAME + " c";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            PaperSearcher paperSearcher = new PaperSearcher();
            ConferenceDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new ConferenceDTO();
                LinkedHashMap<String, String> listPublicationCitation = paperSearcher.getListPublicationCitation(this.path, rs.getString(ConferenceTB.COLUMN_CONFERENCEID), 2);
                ArrayList<Integer> publicationList = paperSearcher.getPublicationList(this.path, rs.getString(ConferenceTB.COLUMN_CONFERENCEID), 2);
                LinkedHashMap<String, Integer> indexConference = Common.getCalculateIndex(publicationList);
                dto.setIdConference(rs.getString(ConferenceTB.COLUMN_CONFERENCEID));
                dto.setConferenceName(rs.getString(ConferenceTB.COLUMN_CONFERENCENAME));
                dto.setDuration(rs.getString(ConferenceTB.COLUMN_DURATION));
                dto.setOrganization(rs.getString(ConferenceTB.COLUMN_ORGANIZATION));
                dto.setOrganizedLocation(rs.getString(ConferenceTB.COLUMN_ORGANIZEDLOCATION));
                dto.setWebsite(rs.getString(ConferenceTB.COLUMN_WEBSITE));
                dto.setYearEnd(rs.getInt(ConferenceTB.COLUMN_YEAREND));
                dto.setYearStart(rs.getInt(ConferenceTB.COLUMN_YEARSTART));
                dto.setListIdSubdomain(this.getListIdSubdomain(connectionPool, rs.getInt(ConferenceTB.COLUMN_CONFERENCEID)));
                if (listPublicationCitation != null) {
                    dto.setCitationCount(Integer.parseInt(listPublicationCitation.get("citationCount")));
                    dto.setPublicationCount(Integer.parseInt(listPublicationCitation.get("publicationCount")));
                    dto.setListPublicationCitation(listPublicationCitation.get("listPublicationCitation"));
                }
                dto.setH_Index(indexConference.get("h_index"));
                dto.setG_Index(indexConference.get("g_index"));

                int pubLast5Year = 0;
                int citLast5Year = 0;
                int g_indexLast5Year = 0;
                int h_indexLast5Year = 0;
                int pubLast10Year = 0;
                int citLast10Year = 0;
                int g_indexLast10Year = 0;
                int h_indexLast10Year = 0;

                LinkedHashMap<String, Object> object10Year = paperSearcher.getPapersForAll(this.path, rs.getString(ConferenceTB.COLUMN_CONFERENCEID), 10, 2);
                if (object10Year != null) {
                    ArrayList<Integer> publicationList10Year = (ArrayList<Integer>) object10Year.get("list");
                    LinkedHashMap<String, Integer> index10Year = Common.getCalculateIndex(publicationList10Year);
                    pubLast10Year = Integer.parseInt(object10Year.get("pubCount").toString());
                    citLast10Year = Integer.parseInt(object10Year.get("citCount").toString());
                    g_indexLast10Year = index10Year.get("g_index");
                    h_indexLast10Year = index10Year.get("h_index");

                    LinkedHashMap<String, Object> object5Year = paperSearcher.getPapersForAll(this.path, rs.getString(ConferenceTB.COLUMN_CONFERENCEID), 5, 2);
                    if (object5Year != null) {
                        ArrayList<Integer> publicationList5Year = (ArrayList<Integer>) object5Year.get("list");
                        LinkedHashMap<String, Integer> index5Year = Common.getCalculateIndex(publicationList5Year);
                        pubLast5Year = Integer.parseInt(object5Year.get("pubCount").toString());
                        citLast5Year = Integer.parseInt(object5Year.get("citCount").toString());
                        g_indexLast5Year = index5Year.get("g_index");
                        h_indexLast5Year = index5Year.get("h_index");
                    }
                }

                Document d = new Document();
                d.add(new Field(IndexConst.CONFERENCE_IDCONFERENCE_FIELD, dto.idConference, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.CONFERENCE_CONFERENCENAME_FIELD, dto.conferenceName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.CONFERENCE_DURATION_FIELD, dto.duration, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.CONFERENCE_ORGANIZATION_FIELD, dto.organization, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.CONFERENCE_ORGANIZEDLOCATION_FIELD, dto.organizedLocation, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.CONFERENCE_WEBSITE_FIELD, dto.website, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.CONFERENCE_LISTIDSUBDOMAIN_FIELD, dto.listIdSubdomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.CONFERENCE_LISTPUBLICATIONCITATION_FIELD, dto.listPublicationCitation, Field.Store.YES, Field.Index.NO));

                d.add(new NumericField(IndexConst.CONFERENCE_YEAREND_FIELD, Field.Store.YES, false).setIntValue(dto.yearEnd));
                d.add(new NumericField(IndexConst.CONFERENCE_YEARSTART_FIELD, Field.Store.YES, false).setIntValue(dto.yearStart));
                d.add(new NumericField(IndexConst.CONFERENCE_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.publicationCount));
                d.add(new NumericField(IndexConst.CONFERENCE_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.citationCount));
                d.add(new NumericField(IndexConst.CONFERENCE_HINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.h_index));
                d.add(new NumericField(IndexConst.CONFERENCE_GINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.g_index));

                d.add(new NumericField(IndexConst.CONFERENCE_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                d.add(new NumericField(IndexConst.CONFERENCE_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                d.add(new NumericField(IndexConst.CONFERENCE_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                d.add(new NumericField(IndexConst.CONFERENCE_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));
                d.add(new NumericField(IndexConst.CONFERENCE_HINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast5Year));
                d.add(new NumericField(IndexConst.CONFERENCE_HINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast10Year));
                d.add(new NumericField(IndexConst.CONFERENCE_GINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast5Year));
                d.add(new NumericField(IndexConst.CONFERENCE_GINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast10Year));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + dto.conferenceName);
                d = null;
                dto = null;
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
     * truy vấn List các lĩnh vực mà hội nghị này có bài viết
     *
     * @param connectionPool: kết nối tới csdl
     * @param idConference: id của 1 conference
     * @return ListId các lĩnh vực
     */
    private String getListIdSubdomain(ConnectionPool connectionPool, int idConference) throws SQLException, ClassNotFoundException {
        String list = "";
        Connection connection = connectionPool.getConnection();
        try {
            String sql = "SELECT s." + SubdomainPaperTB.COLUMN_SUBDOMAINID + " FROM " + PaperTB.TABLE_NAME + " p JOIN " + SubdomainPaperTB.TABLE_NAME + " s ON p." + PaperTB.COLUMN_PAPERID + " = s." + SubdomainPaperTB.COLUMN_PAPERID + " WHERE p." + PaperTB.COLUMN_CONFERENCEID + " = ? GROUP BY s." + SubdomainPaperTB.COLUMN_SUBDOMAINID + "";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idConference);
            ResultSet rs = stmt.executeQuery();
            while ((rs != null) && (rs.next())) {
                list += " " + rs.getString(SubdomainPaperTB.COLUMN_SUBDOMAINID);
            }
            if (!"".equals(list)) {
                list = list.substring(1);
            }
            rs.close();
            stmt.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            connection.close();
        }
        return list;
    }

    /**
     * hàm test index
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
            ConferenceIndexer indexer = new ConferenceIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}