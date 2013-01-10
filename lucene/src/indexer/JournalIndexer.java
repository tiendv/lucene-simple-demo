/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import bo.IndexBO;
import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import database.JournalTB;
import database.PaperTB;
import database.SubdomainPaperTB;
import dto.JournalDTO;
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
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class JournalIndexer {

    private IndexSearcher searcher = null;
    private String path = "E:\\";

    /**
     * hàm khởi tạo searcher
     *
     * @param path: đường dẫn tới thư mục lưu trữ file index
     */
    public JournalIndexer(String path) {
        try {
            FSDirectory directory = Common.getFSDirectory(path, IndexConst.PAPER_INDEX_PATH);
            searcher = new IndexSearcher(directory);
            this.path = path;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
     * hàm khởi chạy index
     *
     * @param connectionPool: kết nối tới csdl
     * @return số lượng doc được thực hiện index và thời gian index
     */
    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.JOURNAL_INDEX_PATH);
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
     * Truy vấn các thông tin của journal trong csdl, truy vấn và tính toán các
     * thuộc tính khác của journal: citationCoun, publicationCount, h-index,
     * g-index
     *
     * @param connectionPool: kết nối csdl
     * @param indexDir: thư mục lưu trữ file index
     * @return số doc được index
     */
    private int _index(ConnectionPool connectionPool, File indexDir) throws IOException, SQLException {
        int count = 0;
        IndexBO indexBO = new IndexBO();
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
        Directory directory = FSDirectory.open(indexDir);
        IndexWriter writer = new IndexWriter(directory, config);
        // Connection to DB
        Connection connection = connectionPool.getConnection();
        try {
            String sql = "SELECT * FROM " + JournalTB.TABLE_NAME + " j";
            PreparedStatement stmt = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery();
            // Index data from query
            JournalDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new JournalDTO();
                LinkedHashMap<String, String> listPublicationCitation = indexBO.getListPublicationCitation(path + IndexConst.PAPER_INDEX_PATH, rs.getString(JournalTB.COLUMN_JOURNALID), 3);
                ArrayList<Integer> publicationList = this.getPublicationList(rs.getString(JournalTB.COLUMN_JOURNALID));
                LinkedHashMap<String, Integer> indexJournal = indexBO.getCalculateIndex(publicationList);
                dto.setIdJournal(rs.getString(JournalTB.COLUMN_JOURNALID));
                dto.setJournalName(rs.getString(JournalTB.COLUMN_JOURNALNAME));
                dto.setOrganization(rs.getString(JournalTB.COLUMN_ORGANIZATION));
                dto.setWebsite(rs.getString(JournalTB.COLUMN_WEBSITE));
                dto.setYearEnd(rs.getInt(JournalTB.COLUMN_YEAREND));
                dto.setYearStart(rs.getInt(JournalTB.COLUMN_YEARSTART));
                dto.setListIdSubdomain(this.getListIdSubdomain(connectionPool, rs.getInt(JournalTB.COLUMN_JOURNALID)));
                if (listPublicationCitation != null) {
                    dto.setCitationCount(Integer.parseInt(listPublicationCitation.get("citationCount")));
                    dto.setPublicationCount(Integer.parseInt(listPublicationCitation.get("publicationCount")));
                    dto.setListPublicationCitation(listPublicationCitation.get("listPublicationCitation"));
                }
                dto.setH_Index(indexJournal.get("h_index"));
                dto.setG_Index(indexJournal.get("g_index"));

                int pubLast5Year = 0;
                int citLast5Year = 0;
                int g_indexLast5Year = 0;
                int h_indexLast5Year = 0;
                int pubLast10Year = 0;
                int citLast10Year = 0;
                int g_indexLast10Year = 0;
                int h_indexLast10Year = 0;

                LinkedHashMap<String, Object> object10Year = indexBO.getPapersForAll(path + IndexConst.PAPER_INDEX_PATH, rs.getString(JournalTB.COLUMN_JOURNALID), 10, 3);
                if (object10Year != null) {
                    ArrayList<Integer> publicationList10Year = (ArrayList<Integer>) object10Year.get("list");
                    LinkedHashMap<String, Integer> index10Year = indexBO.getCalculateIndex(publicationList10Year);
                    pubLast10Year = Integer.parseInt(object10Year.get("pubCount").toString());
                    citLast10Year = Integer.parseInt(object10Year.get("citCount").toString());
                    g_indexLast10Year = index10Year.get("g_index");
                    h_indexLast10Year = index10Year.get("h_index");

                    LinkedHashMap<String, Object> object5Year = indexBO.getPapersForAll(path + IndexConst.PAPER_INDEX_PATH, rs.getString(JournalTB.COLUMN_JOURNALID), 5, 3);
                    if (object5Year != null) {
                        ArrayList<Integer> publicationList5Year = (ArrayList<Integer>) object5Year.get("list");
                        LinkedHashMap<String, Integer> index5Year = indexBO.getCalculateIndex(publicationList5Year);
                        pubLast5Year = Integer.parseInt(object5Year.get("pubCount").toString());
                        citLast5Year = Integer.parseInt(object5Year.get("citCount").toString());
                        g_indexLast5Year = index5Year.get("g_index");
                        h_indexLast5Year = index5Year.get("h_index");
                    }
                }

                Document d = new Document();
                d.add(new Field(IndexConst.JOURNAL_IDJOURNAL_FIELD, dto.idJournal, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.JOURNAL_JOURNALNAME_FIELD, dto.journalName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.JOURNAL_ORGANIZATION_FIELD, dto.organization, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.JOURNAL_WEBSITE_FIELD, dto.website, Field.Store.YES, Field.Index.NO));
                d.add(new Field(IndexConst.JOURNAL_LISTIDSUBDOMAIN_FIELD, dto.listIdSubdomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.JOURNAL_LISTPUBLICATIONCITATION_FIELD, dto.listPublicationCitation, Field.Store.YES, Field.Index.NO));

                d.add(new NumericField(IndexConst.JOURNAL_YEAREND_FIELD, Field.Store.YES, false).setIntValue(dto.yearEnd));
                d.add(new NumericField(IndexConst.JOURNAL_YEARSTART_FIELD, Field.Store.YES, false).setIntValue(dto.yearStart));
                d.add(new NumericField(IndexConst.JOURNAL_PUBLICATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.publicationCount));
                d.add(new NumericField(IndexConst.JOURNAL_CITATIONCOUNT_FIELD, Field.Store.YES, true).setIntValue(dto.citationCount));
                d.add(new NumericField(IndexConst.JOURNAL_HINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.h_index));
                d.add(new NumericField(IndexConst.JOURNAL_GINDEX_FIELD, Field.Store.YES, true).setIntValue(dto.g_index));

                d.add(new NumericField(IndexConst.JOURNAL_PUBLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast5Year));
                d.add(new NumericField(IndexConst.JOURNAL_PUBLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(pubLast10Year));
                d.add(new NumericField(IndexConst.JOURNAL_CITLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast5Year));
                d.add(new NumericField(IndexConst.JOURNAL_CITLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(citLast10Year));
                d.add(new NumericField(IndexConst.JOURNAL_HINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast5Year));
                d.add(new NumericField(IndexConst.JOURNAL_HINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(h_indexLast10Year));
                d.add(new NumericField(IndexConst.JOURNAL_GINDEXLAST5YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast5Year));
                d.add(new NumericField(IndexConst.JOURNAL_GINDEXLAST10YEAR_FIELD, Field.Store.YES, true).setIntValue(g_indexLast10Year));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + dto.journalName);
                d = null;
                dto = null;
            }
            rs.close();
            stmt.close();
            count = writer.numDocs();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            connection.close();
            writer.optimize();
            writer.close();
            directory.close();
        }
        return count;
    }

    /**
     * hàm lấy chuỗi idsubdomain mà journal có bài trong đó
     *
     * @param connectionPool: kết nối csdl
     * @param idJournal
     * @return list các idsubdomain
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private String getListIdSubdomain(ConnectionPool connectionPool, int idJournal) throws SQLException, ClassNotFoundException {
        String list = "";
        Connection connection = connectionPool.getConnection();
        try {
            String sql = "SELECT s." + SubdomainPaperTB.COLUMN_SUBDOMAINID + " FROM " + PaperTB.TABLE_NAME + " p JOIN " + SubdomainPaperTB.TABLE_NAME + " s ON p." + PaperTB.COLUMN_PAPERID + " = s." + SubdomainPaperTB.COLUMN_PAPERID + " WHERE p." + PaperTB.COLUMN_JOURNALID + " = ? GROUP BY s." + SubdomainPaperTB.COLUMN_SUBDOMAINID + "";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setInt(1, idJournal);
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
            System.out.println(ex.getMessage());
        } finally {
            connection.close();
        }
        return list;
    }

    /**
     * Truy vấn số lượng citation của các bài viết trong journal và sắp xếp từ
     * nhiều đến ít
     *
     * @param idJournal
     * @return array số lượng citation
     */
    private ArrayList<Integer> getPublicationList(String idJournal) throws IOException, ParseException {
        ArrayList<Integer> publicationList = new ArrayList<Integer>();
        BooleanQuery booleanQuery = new BooleanQuery();
        QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDJOURNAL_FIELD, new StandardAnalyzer(Version.LUCENE_36));
        Query query = parser.parse(idJournal);
        booleanQuery.add(query, BooleanClause.Occur.MUST);
        Sort sort = new Sort(new SortField[]{
                    new SortField(IndexConst.PAPER_CITATIONCOUNT_FIELD, SortField.INT, true)});
        TopDocs result = searcher.search(booleanQuery, Integer.MAX_VALUE, sort);
        if (result != null) {
            ScoreDoc[] hits = result.scoreDocs;
            for (int i = 0; i < result.totalHits; i++) {
                ScoreDoc hit = hits[i];
                Document doc = searcher.doc(hit.doc);
                publicationList.add(Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD)));
            }
        }
        return publicationList;
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
            JournalIndexer indexer = new JournalIndexer(path);
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}