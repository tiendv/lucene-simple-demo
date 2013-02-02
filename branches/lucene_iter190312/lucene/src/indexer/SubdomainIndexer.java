/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import constant.IndexConst;
import database.SubdomainTB;
import dto.SubdomainDTO;
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
public class SubdomainIndexer {

    private String path = "E:\\INDEX\\";

    /**
     * hàm khởi tạo
     *
     * @param path: đường dẫn tới thư mục chứa file index
     */
    public SubdomainIndexer(String path) {
        this.path = path;
    }

    /**
     * hàm khởi chạy index
     *
     * @param connectionPool: kết nối csdl
     * @return số doc thực hiện index và thời gian index
     */
    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.SUBDOMAIN_INDEX_PATH);
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
     * truy vấn từ csdl các thông tin của subdomain, gọi các hàm truy vấn và
     * tính toán các chỉ số, index
     *
     * @param connectionPool: kết nối csdl
     * @param indexDir: thư mục chứa file index
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
            String sql = "SELECT * FROM " + SubdomainTB.TABLE_NAME + " s";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            PaperSearcher paperSearcher = new PaperSearcher();
            SubdomainDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new SubdomainDTO();
                LinkedHashMap<String, String> listPublicationCitation = paperSearcher.getListPublicationCitation(path, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), 6);
                dto.setIdSubdomain(rs.getString(SubdomainTB.COLUMN_SUBDOMAINID));
                dto.setSubdomainName(rs.getString(SubdomainTB.COLUMN_SUBDOMAINNAME));
                dto.setIdDomain(rs.getString(SubdomainTB.COLUMN_DOMAINID));
                if (listPublicationCitation != null) {
                    //dto.setCitationCount(Integer.parseInt(listPublicationCitation.get("citationCount")));
                    //dto.setPublicationCount(Integer.parseInt(listPublicationCitation.get("publicationCount")));
                    dto.setListPublicationCitation(listPublicationCitation.get("listPublicationCitation"));
                }

                int pubLast5Year = 0;
                int citLast5Year = 0;
                int pubLast10Year = 0;
                int citLast10Year = 0;
                int publicationCount = 0;
                int citationCount = 0;

                LinkedHashMap<String, Integer> objectAllYear = paperSearcher.getPublicationsFromIdSubdomain(path, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), 0);
                if (objectAllYear != null) {
                    publicationCount = objectAllYear.get("pubCount");
                    citationCount = objectAllYear.get("citCount");
                    LinkedHashMap<String, Integer> object10Year = paperSearcher.getPublicationsFromIdSubdomain(path, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), 10);
                    if (object10Year != null) {
                        pubLast10Year = object10Year.get("pubCount");
                        citLast10Year = object10Year.get("citCount");
                        LinkedHashMap<String, Integer> object5Year = paperSearcher.getPublicationsFromIdSubdomain(path, rs.getString(SubdomainTB.COLUMN_SUBDOMAINID), 5);
                        if (object5Year != null) {
                            pubLast5Year = object5Year.get("pubCount");
                            citLast5Year = object5Year.get("citCount");
                        }
                    }
                }

                Document d = new Document();
                d.add(new Field(IndexConst.SUBDOMAIN_IDSUBDOMAIN_FIELD, dto.idSubdomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.SUBDOMAIN_SUBDOMAINNAME_FIELD, dto.subdomainName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.SUBDOMAIN_IDDOMAIN_FIELD, dto.idDomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.SUBDOMAIN_LISTPUBLICATIONCITATION_FIELD, dto.listPublicationCitation, Field.Store.YES, Field.Index.NO));

                //d.add(new NumericField(IndexConst.SUBDOMAIN_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.publicationCount));
                //d.add(new NumericField(IndexConst.SUBDOMAIN_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.citationCount));
                d.add(new NumericField(IndexConst.SUBDOMAIN_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                d.add(new NumericField(IndexConst.SUBDOMAIN_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                d.add(new NumericField(IndexConst.SUBDOMAIN_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                d.add(new NumericField(IndexConst.SUBDOMAIN_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));
                d.add(new NumericField(IndexConst.SUBDOMAIN_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(publicationCount));
                d.add(new NumericField(IndexConst.SUBDOMAIN_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(citationCount));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + dto.subdomainName);
                d = null;
                dto = null;
            }
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
            SubdomainIndexer indexer = new SubdomainIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}