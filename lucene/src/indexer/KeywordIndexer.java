/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import constant.IndexConst;
import database.KeywordTB;
import database.PaperKeywordTB;
import database.SubdomainPaperTB;
import dto.KeywordDTO;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
public class KeywordIndexer {

    private String path = "E:\\INDEX\\";

    /**
     * khởi tạo searcher
     *
     * @param path: đường dẫn thư mục lưu trữ file index
     */
    public KeywordIndexer(String path) {
        this.path = path;
    }

    /**
     * hàm khởi chạy index
     *
     * @param connectionPool kết nối tới csdl
     * @return số doc thực hiện index, và thời gian index
     */
    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.KEYWORD_INDEX_PATH);
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
     * Thực hiện truy vấn các thông tin về từ khóa trong csdl, đối với mỗi từ
     * khóa thực hiện truy vấn và tính toán các chỉ số
     *
     * @param connectionPool: kết nối csdl
     * @param indexDir: thư mục lưu trữ file index
     * @return số doc thực hiện index
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
            String sql = "SELECT * FROM " + KeywordTB.TABLE_NAME + " k";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            PaperSearcher paperSearcher = new PaperSearcher();
            KeywordDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new KeywordDTO();
                LinkedHashMap<String, String> listPublicationCitation = paperSearcher.getListPublicationCitation(path, rs.getString(KeywordTB.COLUMN_KEYWORDID), 5);
                dto.setIdKeyword(rs.getString(KeywordTB.COLUMN_KEYWORDID));
                dto.setKeyword(rs.getString(KeywordTB.COLUMN_KEYWORD));
                dto.setStemmingVariations(rs.getString(KeywordTB.COLUMN_STEMMINGVARIATIONS));
                dto.setListIdSubdomain(this.getListIdSubdomain(connectionPool, rs.getInt(KeywordTB.COLUMN_KEYWORDID)));
                if (listPublicationCitation != null) {
                    dto.setCitationCount(Integer.parseInt(listPublicationCitation.get("citationCount")));
                    dto.setPublicationCount(Integer.parseInt(listPublicationCitation.get("publicationCount")));
                    dto.setListPublicationCitation(listPublicationCitation.get("listPublicationCitation"));
                }

                int pubLast5Year = 0;
                int citLast5Year = 0;
                int pubLast10Year = 0;
                int citLast10Year = 0;

                LinkedHashMap<String, Object> object10Year = paperSearcher.getPapersForAll(path, rs.getString(KeywordTB.COLUMN_KEYWORDID), 10, 5);
                if (object10Year != null) {
                    pubLast10Year = Integer.parseInt(object10Year.get("pubCount").toString());
                    citLast10Year = Integer.parseInt(object10Year.get("citCount").toString());

                    LinkedHashMap<String, Object> object5Year = paperSearcher.getPapersForAll(path, rs.getString(KeywordTB.COLUMN_KEYWORDID), 5, 5);
                    if (object5Year != null) {
                        pubLast5Year = Integer.parseInt(object5Year.get("pubCount").toString());
                        citLast5Year = Integer.parseInt(object5Year.get("citCount").toString());
                    }
                }

                Document d = new Document();
                d.add(new Field(IndexConst.KEYWORD_IDKEYWORD_FIELD, dto.idKeyword, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.KEYWORD_KEYWORD_FIELD, dto.keyword, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.KEYWORD_STEMMINGVARIATIONS_FIELD, dto.stemmingVariations, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.KEYWORD_LISTIDSUBDOMAIN_FIELD, dto.listIdSubdomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.KEYWORD_LISTPUBLICATIONCITATION_FIELD, dto.listPublicationCitation, Field.Store.YES, Field.Index.NO));

                d.add(new NumericField(IndexConst.KEYWORD_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.publicationCount));
                d.add(new NumericField(IndexConst.KEYWORD_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.citationCount));

                d.add(new NumericField(IndexConst.KEYWORD_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                d.add(new NumericField(IndexConst.KEYWORD_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                d.add(new NumericField(IndexConst.KEYWORD_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                d.add(new NumericField(IndexConst.KEYWORD_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + dto.keyword);
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
     * Truy vấn chuỗi id các subdomain mà các bài báo trong đó có dử dụng từ
     * khóa
     *
     * @param connectionPool: kết nối csdl
     * @param idKeyword: id keyword
     * @return list các idSubdomain
     */
    private String getListIdSubdomain(ConnectionPool connectionPool, int idKeyword) throws SQLException, ClassNotFoundException {
        String list = "";
        Connection connection = connectionPool.getConnection();
        try {
            String sql = "SELECT sp." + SubdomainPaperTB.COLUMN_SUBDOMAINID + " FROM " + PaperKeywordTB.TABLE_NAME + " pk JOIN " + SubdomainPaperTB.TABLE_NAME + " sp ON sp." + SubdomainPaperTB.COLUMN_PAPERID + " = pk." + PaperKeywordTB.COLUMN_PAPERID + " WHERE pk." + PaperKeywordTB.COLUMN_KEYWORDID + " = ? GROUP BY sp." + SubdomainPaperTB.COLUMN_SUBDOMAINID + "";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idKeyword);
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
            KeywordIndexer indexer = new KeywordIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}