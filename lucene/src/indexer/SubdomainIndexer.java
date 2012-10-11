/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.Common;
import constant.ConnectionPool;
import constant.IndexConst;
import constant.PubCiComparator;
import database.SubdomainTB;
import dto.PubCiDTO;
import dto.SubdomainDTO;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class SubdomainIndexer {

    private ConnectionPool connectionPool;
    private IndexSearcher searcher = null;
    private String path = null;
    public Boolean connect = true;
    public Boolean folder = true;

    public SubdomainIndexer(String username, String password, String database, int port, String path) {
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
            File indexDir = new File(path + IndexConst.SUBDOMAIN_INDEX_PATH);
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
            SubdomainDTO dto = null;
            while ((rs != null) && (rs.next())) {
                dto = new SubdomainDTO();
                Document d = new Document();
                LinkedHashMap<String, String> listPublicationCitation = this.getListPublicationCitation(rs.getString(SubdomainTB.COLUMN_SUBDOMAINID));
                dto.setIdSubdomain(rs.getString(SubdomainTB.COLUMN_SUBDOMAINID));
                dto.setSubdomainName(rs.getString(SubdomainTB.COLUMN_SUBDOMAINNAME));
                dto.setIdDomain(rs.getString(SubdomainTB.COLUMN_DOMAINID));
                dto.setCitationCount(Integer.parseInt(listPublicationCitation.get("citationCount")));
                dto.setPublicationCount(Integer.parseInt(listPublicationCitation.get("publicationCount")));
                dto.setListPublicationCitation(listPublicationCitation.get("listPublicationCitation"));

                d.add(new Field(IndexConst.SUBDOMAIN_IDSUBDOMAIN_FIELD, dto.idSubdomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.SUBDOMAIN_SUBDOMAINNAME_FIELD, dto.subdomainName, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.SUBDOMAIN_IDDOMAIN_FIELD, dto.idDomain, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new Field(IndexConst.SUBDOMAIN_LISTPUBLICATIONCITATION_FIELD, dto.listPublicationCitation, Field.Store.YES, Field.Index.NO));
                d.add(new NumericField(IndexConst.SUBDOMAIN_PUBLICATIONCOUNT_FIELD, Field.Store.YES, false).setIntValue(dto.publicationCount));
                d.add(new NumericField(IndexConst.SUBDOMAIN_CITATIONCOUNT_FIELD, Field.Store.YES, false).setIntValue(dto.citationCount));

                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t" + dto.subdomainName);
                d = null;
                dto = null;
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

    public LinkedHashMap<String, String> getListPublicationCitation(String idSubdomain) throws IOException, ParseException {
        LinkedHashMap<String, String> out = new LinkedHashMap<String, String>();
        BooleanQuery booleanQuery = new BooleanQuery();
        QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
        Query query = parser.parse(idSubdomain);
        booleanQuery.add(query, BooleanClause.Occur.MUST);
        TopDocs result = searcher.search(booleanQuery, Integer.MAX_VALUE);
        if (result != null) {
            ScoreDoc[] hits = result.scoreDocs;
            ArrayList<PubCiDTO> pubCiDTOList = new ArrayList<PubCiDTO>();
            int citationCount = 0;
            for (int i = 0; i < result.totalHits; i++) {
                ScoreDoc hit = hits[i];
                Document doc = searcher.doc(hit.doc);
                citationCount += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
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
            out.put("publicationCount", Integer.toString(result.totalHits));
            out.put("citationCount", Integer.toString(citationCount));
            out.put("listPublicationCitation", Common.OToS(listPublicationCitation));
        }
        return out;
    }

    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String user = "root";
            String pass = "@huydang1920@";
            String database = "cspublicationcrawler";
            int port = 3306;
            String path = "E:\\";
            SubdomainIndexer indexer = new SubdomainIndexer(user, pass, database, port, path);
            System.out.println(indexer._run());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}