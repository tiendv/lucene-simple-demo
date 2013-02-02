/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import constant.PubCiComparator;
import database.AuthorPaperTB;
import database.AuthorTB;
import database.SubdomainPaperTB;
import dto.AuthorDTO;
import dto.PubCiDTO;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import searcher.PaperSearcher;

/**
 *
 * @author HuyDang
 */
public class AuthorIndexer {

    private String path = "E:\\INDEX\\";

    public AuthorIndexer(String path) {
        this.path = path;
    }

    /**
     *
     * @param connectionPool: kết nối connection của MySQL
     * @Summary: khởi chạy index
     * @return: chuỗi out: số lượng doc được index và thời gian index
     */
    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.AUTHOR_INDEX_PATH);
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
     *
     * @param connectionPool: kết nối connection của MySQL
     * @param indexDir: thư mục lưu trữ file Index
     * @Summary:Truy vấn trong csdl MySQL lấy ra thông tin tất cả các tác giả,
     * sử dụng IdAuthor để truy vấn và tính toán các thuộc tính.
     * @return: trả về số doc được index
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
            String sql = "SELECT * FROM " + AuthorTB.TABLE_NAME + " a LIMIT 10";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            PaperSearcher paperSearcher = new PaperSearcher();
            AuthorDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new AuthorDTO();
                LinkedHashMap<String, String> listPublicationCitation = this.getListPublicationCitation(connectionPool, rs.getString(AuthorTB.COLUMN_AUTHORID));
                ArrayList<Integer> publicationList = paperSearcher.getPublicationList(this.path, rs.getString(AuthorTB.COLUMN_AUTHORID), 1);
                LinkedHashMap<String, Integer> indexAuthor = Common.getCalculateIndex(publicationList);
                dto.setIdAuthor(rs.getString(AuthorTB.COLUMN_AUTHORID));
                dto.setAuthorName(rs.getString(AuthorTB.COLUMN_AUTHORNAME));
                dto.setIdOrg(rs.getString(AuthorTB.COLUMN_ORGID));
                dto.setImage(rs.getString(AuthorTB.COLUMN_IMAGE));
                dto.setWebsite(rs.getString(AuthorTB.COLUMN_WEBSITE));
                dto.setPublicationCount(Integer.parseInt(listPublicationCitation.get("publicationCount")));
                dto.setCitationCount(Integer.parseInt(listPublicationCitation.get("citationCount")));
                dto.setCoAuthorCount(Integer.parseInt(listPublicationCitation.get("coAuthorCount")));
                dto.setListPublicationCitation(listPublicationCitation.get("listPublicationCitation"));
                dto.setListIdSubdomain(this.getListIdSubdomain(connectionPool, rs.getInt(AuthorTB.COLUMN_AUTHORID)));
                dto.setH_Index(indexAuthor.get("h_index"));
                dto.setG_Index(indexAuthor.get("g_index"));
                dto.setRank(0);

                int pubLast5Year = 0;
                int citLast5Year = 0;
                int g_indexLast5Year = 0;
                int h_indexLast5Year = 0;
                int pubLast10Year = 0;
                int citLast10Year = 0;
                int g_indexLast10Year = 0;
                int h_indexLast10Year = 0;

                LinkedHashMap<String, Object> object10Year = paperSearcher.getPapersForAll(path, rs.getString(AuthorTB.COLUMN_AUTHORID), 10, 1);
                if (object10Year != null) {
                    ArrayList<Integer> publicationList10Year = (ArrayList<Integer>) object10Year.get("list");
                    LinkedHashMap<String, Integer> index10Year = Common.getCalculateIndex(publicationList10Year);
                    pubLast10Year = Integer.parseInt(object10Year.get("pubCount").toString());
                    citLast10Year = Integer.parseInt(object10Year.get("citCount").toString());
                    g_indexLast10Year = index10Year.get("g_index");
                    h_indexLast10Year = index10Year.get("h_index");

                    LinkedHashMap<String, Object> object5Year = paperSearcher.getPapersForAll(path, rs.getString(AuthorTB.COLUMN_AUTHORID), 5, 1);
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
                d.add(new Field(IndexConst.AUTHOR_IDAUTHOR_FIELD, dto.idAuthor, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_AUTHORNAME_FIELD, dto.authorName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_IDORG_FIELD, dto.idOrg, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_IMAGE_FIELD, dto.image, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.AUTHOR_WEBSITE_FIELD, dto.website, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_LISTIDSUBDOMAIN_FIELD, dto.listIdSubdomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.AUTHOR_LISTPUBLICATIONCITATION_FIELD, dto.listPublicationCitation, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new NumericField(IndexConst.AUTHOR_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.publicationCount));
                d.add(new NumericField(IndexConst.AUTHOR_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.citationCount));
                d.add(new NumericField(IndexConst.AUTHOR_COAUTHORCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.coAuthorCount));
                d.add(new NumericField(IndexConst.AUTHOR_HINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.h_index));
                d.add(new NumericField(IndexConst.AUTHOR_GINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.g_index));
                d.add(new NumericField(IndexConst.AUTHOR_RANK_FIELD, Field.Store.YES, true).setIntValue(dto.rank));

                d.add(new NumericField(IndexConst.AUTHOR_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                d.add(new NumericField(IndexConst.AUTHOR_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                d.add(new NumericField(IndexConst.AUTHOR_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                d.add(new NumericField(IndexConst.AUTHOR_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));
                d.add(new NumericField(IndexConst.AUTHOR_HINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast5Year));
                d.add(new NumericField(IndexConst.AUTHOR_HINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast10Year));
                d.add(new NumericField(IndexConst.AUTHOR_GINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast5Year));
                d.add(new NumericField(IndexConst.AUTHOR_GINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast10Year));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + dto.authorName);
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
     *
     * @param connectionPool kết nối tới csdl
     * @param idAuthor: idAuthor truy vấn
     * @return List IdSubdomain của tác giả
     * @Summary: truy vấn lấy ra chuỗi Id các subdomain mà tác giả có bài viết
     */
    private String getListIdSubdomain(ConnectionPool connectionPool, int idAuthor) throws SQLException, ClassNotFoundException, IOException, ParseException {
        String list = "";
        Connection connection = connectionPool.getConnection();
        try {
            String sql = "SELECT sp." + SubdomainPaperTB.COLUMN_SUBDOMAINID + " FROM " + AuthorPaperTB.TABLE_NAME + " ap JOIN " + SubdomainPaperTB.TABLE_NAME + " sp ON ap." + AuthorPaperTB.COLUMN_PAPERID + " = sp." + SubdomainPaperTB.COLUMN_PAPERID + " WHERE ap." + AuthorPaperTB.COLUMN_AUTHORID + " = ? GROUP BY sp." + SubdomainPaperTB.COLUMN_SUBDOMAINID + "";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idAuthor);
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
     *
     * @param connectionPool kết nối tới csdl
     * @param idAuthor: idAuthor truy vấn
     * @return LinkedHashMap bao gồm:PublicationCount, CitationCount, List
     * co-Author, listPublicationCitation(Publication, citation theo thời gian)
     * của tác giả
     */
    private LinkedHashMap<String, String> getListPublicationCitation(ConnectionPool connectionPool, String idAuthor) throws IOException, ParseException, SQLException, ClassNotFoundException {
        LinkedHashMap<String, String> out = new LinkedHashMap<String, String>();
        BooleanQuery booleanQuery = new BooleanQuery();
        QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDAUTHOR_FIELD, new StandardAnalyzer(Version.LUCENE_36));
        Query query = parser.parse(idAuthor);
        booleanQuery.add(query, BooleanClause.Occur.MUST);
        TopDocs result = PaperSearcher.getSearcher(path).search(booleanQuery, Integer.MAX_VALUE);
        if (result != null) {
            ScoreDoc[] hits = result.scoreDocs;
            ArrayList<PubCiDTO> pubCiDTOList = new ArrayList<PubCiDTO>();
            int citationCount = 0;
            int coAuthorCount = 0;
            String listIdPaper = "";
            for (int i = 0; i < result.totalHits; i++) {
                ScoreDoc hit = hits[i];
                Document doc = PaperSearcher.getSearcher(path).doc(hit.doc);
                citationCount += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                listIdPaper += "," + doc.get(IndexConst.PAPER_IDPAPER_FIELD);
                ArrayList<Object> listCitations = (ArrayList<Object>) Common.SToO(doc.get(IndexConst.PAPER_LISTCITATION_FIELD));
                Iterator it = listCitations.iterator();
                while (it.hasNext()) {
                    LinkedHashMap<String, Integer> temp = (LinkedHashMap<String, Integer>) it.next();
                    if (pubCiDTOList.isEmpty()) {
                        PubCiDTO dto = new PubCiDTO();
                        dto.setCitation(temp.get("citation"));
                        dto.setPublication(0);
                        dto.setYear(temp.get("year"));
                        pubCiDTOList.add(dto);
                    } else {
                        Boolean flag = true;
                        for (int j = 0; j < pubCiDTOList.size(); j++) {
                            if (temp.get("year") == pubCiDTOList.get(j).getYear()) {
                                pubCiDTOList.get(j).setCitation(pubCiDTOList.get(j).getCitation() + temp.get("citation"));
                                flag = false;
                                break;
                            }
                        }
                        if (flag) {
                            PubCiDTO dto = new PubCiDTO();
                            dto.setCitation(temp.get("citation"));
                            dto.setPublication(0);
                            dto.setYear(temp.get("year"));
                            pubCiDTOList.add(dto);
                        }
                    }
                }
                if (Integer.parseInt(doc.get(IndexConst.PAPER_YEAR_FIELD)) == 0) {
                    continue;
                }
                if (pubCiDTOList.isEmpty()) {
                    PubCiDTO dto = new PubCiDTO();
                    dto.setCitation(0);
                    dto.setPublication(1);
                    dto.setYear(Integer.parseInt(doc.get(IndexConst.PAPER_YEAR_FIELD)));
                    pubCiDTOList.add(dto);
                } else {
                    Boolean flag = true;
                    for (int j = 0; j < pubCiDTOList.size(); j++) {
                        if (Integer.parseInt(doc.get(IndexConst.PAPER_YEAR_FIELD)) == pubCiDTOList.get(j).getYear()) {
                            pubCiDTOList.get(j).setPublication(pubCiDTOList.get(j).getPublication() + 1);
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        PubCiDTO dto = new PubCiDTO();
                        dto.setCitation(0);
                        dto.setPublication(1);
                        dto.setYear(Integer.parseInt(doc.get(IndexConst.PAPER_YEAR_FIELD)));
                        pubCiDTOList.add(dto);
                    }
                }
            }
            Collections.sort(pubCiDTOList, new PubCiComparator());
            ArrayList<Object> listPublicationCitation = new ArrayList<Object>();
            for (int i = 0; i < pubCiDTOList.size(); i++) {
                LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
                temp.put("publication", pubCiDTOList.get(i).getPublication());
                temp.put("citation", pubCiDTOList.get(i).getCitation());
                temp.put("year", pubCiDTOList.get(i).getYear());
                listPublicationCitation.add(temp);
            }
            if (!"".equals(listIdPaper)) {
                listIdPaper = listIdPaper.substring(1);
                coAuthorCount = this.getCoAuthorCount(connectionPool, listIdPaper);
            }
            out.put("publicationCount", Integer.toString(result.totalHits));
            out.put("citationCount", Integer.toString(citationCount));
            out.put("coAuthorCount", Integer.toString(coAuthorCount));
            out.put("listPublicationCitation", Common.OToS(listPublicationCitation));
        }
        return out;
    }

    /**
     *
     * @param connectionPool kết nối tới csdl
     * @param listIdPaper list chứa các idpaper của tác giả
     * @return số co-AuthorCount của tác giả
     */
    private int getCoAuthorCount(ConnectionPool connectionPool, String listIdPaper) throws SQLException, ClassNotFoundException, IOException {
        int count = 0;
        Connection connection = connectionPool.getConnection();
        try {
            String sql = "SELECT COUNT(DISTINCT ap." + AuthorPaperTB.COLUMN_AUTHORID + ") AS CoAuthorCount FROM " + AuthorPaperTB.TABLE_NAME + " ap WHERE ap." + AuthorPaperTB.COLUMN_PAPERID + " IN (" + listIdPaper + ")";
            PreparedStatement stmt = connection.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            if ((rs != null) && (rs.next())) {
                count = rs.getInt("CoAuthorCount");
            }
            if (count > 0) {
                count = count - 1;
            }
            rs.close();
            stmt.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            connection.close();
        }
        return count;
    }

    /**
     *
     * @Summary: hàm test index
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
            AuthorIndexer indexer = new AuthorIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}