/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import database.AuthorPaperTB;
import database.AuthorTB;
import database.OrgTB;
import database.SubdomainPaperTB;
import dto.OrgDTO;
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
public class OrgIndexer {

    private String path = "E:\\INDEX\\";

    /**
     * khởi tạo searcher
     *
     * @param path: đường dẫn tới thư mục lưu trữ file index
     */
    public OrgIndexer(String path) {
        this.path = path;
    }

    /**
     * hàm khởi chạy index
     *
     * @param connectionPool: kết nối csdl
     * @return số lượng doc được thực hiện index, thời gian index
     */
    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            File indexDir = new File(this.path + IndexConst.ORG_INDEX_PATH);
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
     * Truy vấn các thông tin của tổ chức từ csdl và gọi các hàm tính toán các
     * chỉ số, thực hiện index
     *
     * @param connectionPool: kết nối csdl
     * @param indexDir: thư mục lưu trữ file index
     * @return số doc thực hiện
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
            String sql = "SELECT * FROM " + OrgTB.TABLE_NAME + " o";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            PaperSearcher paperSearcher = new PaperSearcher();
            OrgDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new OrgDTO();
                LinkedHashMap<String, String> listPublicationCitation = paperSearcher.getListPublicationCitation(this.path, rs.getString(OrgTB.COLUMN_ORGID), 4);
                ArrayList<Integer> publicationList = paperSearcher.getPublicationList(this.path, rs.getString(OrgTB.COLUMN_ORGID), 4);
                LinkedHashMap<String, Integer> indexOrg = Common.getCalculateIndex(publicationList);
                dto.setIdOrg(rs.getString(OrgTB.COLUMN_ORGID));
                dto.setOrgName(rs.getString(OrgTB.COLUMN_ORGNAME));
                dto.setContinent(rs.getString(OrgTB.COLUMN_CONTINENT));
                dto.setIdOrgParent(rs.getString(OrgTB.COLUMN_ORGPARENTID));
                dto.setWebsite(rs.getString(OrgTB.COLUMN_WEBSITE));
                dto.setListIdSubdomain(this.getListIdSubdomain(connectionPool, rs.getInt(OrgTB.COLUMN_ORGID)));
                dto.setCitationCount(Integer.parseInt(listPublicationCitation.get("citationCount")));
                dto.setPublicationCount(Integer.parseInt(listPublicationCitation.get("publicationCount")));
                dto.setListPublicationCitation(listPublicationCitation.get("listPublicationCitation"));
                dto.setH_Index(indexOrg.get("h_index"));
                dto.setG_Index(indexOrg.get("g_index"));

                int pubLast5Year = 0;
                int citLast5Year = 0;
                int g_indexLast5Year = 0;
                int h_indexLast5Year = 0;
                int pubLast10Year = 0;
                int citLast10Year = 0;
                int g_indexLast10Year = 0;
                int h_indexLast10Year = 0;

                LinkedHashMap<String, Object> object10Year = paperSearcher.getPapersForAll(this.path, rs.getString(OrgTB.COLUMN_ORGID), 10, 4);
                if (object10Year != null) {
                    ArrayList<Integer> publicationList10Year = (ArrayList<Integer>) object10Year.get("list");
                    LinkedHashMap<String, Integer> index10Year = Common.getCalculateIndex(publicationList10Year);
                    pubLast10Year = Integer.parseInt(object10Year.get("pubCount").toString());
                    citLast10Year = Integer.parseInt(object10Year.get("citCount").toString());
                    g_indexLast10Year = index10Year.get("g_index");
                    h_indexLast10Year = index10Year.get("h_index");

                    LinkedHashMap<String, Object> object5Year = paperSearcher.getPapersForAll(this.path, rs.getString(OrgTB.COLUMN_ORGID), 5, 4);
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
                d.add(new Field(IndexConst.ORG_IDORG_FIELD, dto.idOrg, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.ORG_ORGNAME_FIELD, dto.orgName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.ORG_CONTINENT_FIELD, dto.continent, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.ORG_WEBSITE_FIELD, dto.website, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.ORG_IDORGPARENT_FIELD, dto.idOrgParent, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.ORG_LISTIDSUBDOMAIN_FIELD, dto.listIdSubdomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.ORG_LISTPUBLICATIONCITATION_FIELD, dto.listPublicationCitation, Field.Store.YES, Field.Index.NO));

                d.add(new NumericField(IndexConst.ORG_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.publicationCount));
                d.add(new NumericField(IndexConst.ORG_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.citationCount));
                d.add(new NumericField(IndexConst.ORG_HINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.h_index));
                d.add(new NumericField(IndexConst.ORG_GINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.g_index));

                d.add(new NumericField(IndexConst.ORG_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                d.add(new NumericField(IndexConst.ORG_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                d.add(new NumericField(IndexConst.ORG_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                d.add(new NumericField(IndexConst.ORG_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));
                d.add(new NumericField(IndexConst.ORG_HINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast5Year));
                d.add(new NumericField(IndexConst.ORG_HINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast10Year));
                d.add(new NumericField(IndexConst.ORG_GINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast5Year));
                d.add(new NumericField(IndexConst.ORG_GINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast10Year));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + dto.orgName);
                d = null;
                dto = null;
            }
            count = writer.numDocs();
            rs.close();
            stmt.close();
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
     * truy vấn lấy ra danh sách các domain mà tổ chức có viết bài
     *
     * @param connectionPool
     * @param idOrg
     * @return list các idSubdomain
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private String getListIdSubdomain(ConnectionPool connectionPool, int idOrg) throws SQLException, ClassNotFoundException {
        Connection connection = connectionPool.getConnection();
        String list = "";
        try {
            String sql = "SELECT s." + SubdomainPaperTB.COLUMN_SUBDOMAINID + " FROM " + AuthorPaperTB.TABLE_NAME + " ap JOIN " + SubdomainPaperTB.TABLE_NAME + " s ON s." + SubdomainPaperTB.COLUMN_PAPERID + "=ap." + AuthorPaperTB.COLUMN_PAPERID + " JOIN " + AuthorTB.TABLE_NAME + " a ON ap." + AuthorPaperTB.COLUMN_AUTHORID + " = a." + AuthorTB.COLUMN_AUTHORID + " WHERE a." + AuthorTB.COLUMN_ORGID + "=? GROUP BY s." + SubdomainPaperTB.COLUMN_SUBDOMAINID + "";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idOrg);
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
            OrgIndexer indexer = new OrgIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
