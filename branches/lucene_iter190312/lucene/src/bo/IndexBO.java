/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bo;

import constant.IndexConst;
import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author HuyDang
 */
public class IndexBO {

    public IndexBO() {
    }

    /**
     * @param path (path lucene index)
     * @param querySearch
     * @param last
     * @param
     * field//1:idAuthor||2:idConference||3:idJournal||4:idOrg||5:idKeyword
     * @return publicationCount, citationCount and publicationList(for H-Index
     * and G-Index) for last
     * @throws Exception
     */
    public LinkedHashMap<String, Object> getPapersForAll(String path, String querySearch, int last, int field) throws Exception {
        try {
            IndexSearcher searcher = new IndexSearcher(getFSDirectory(path));
            LinkedHashMap<String, Object> out = null;
            BooleanQuery blQuery = new BooleanQuery();
            Sort sort = null;
            TopDocs topDocs = null;
            // querySearch
            if (field == 1) {// Author
                QueryParser parserAuthor = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDAUTHOR_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryAuthor = parserAuthor.parse(querySearch);
                blQuery.add(queryAuthor, BooleanClause.Occur.MUST);
            } else if (field == 2) {// Conference
                QueryParser parserConference = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDCONFERENCE_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryConference = parserConference.parse(querySearch);
                blQuery.add(queryConference, BooleanClause.Occur.MUST);
            } else if (field == 3) {//Journal
                QueryParser parserJournal = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDJOURNAL_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryJournal = parserJournal.parse(querySearch);
                blQuery.add(queryJournal, BooleanClause.Occur.MUST);
            } else if (field == 4) {// Org
                QueryParser parserOrg = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDORG_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryOrg = parserOrg.parse(querySearch);
                blQuery.add(queryOrg, BooleanClause.Occur.MUST);
            } else if (field == 5) {// Keyword
                QueryParser parserOrg = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDKEYWORD_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryOrg = parserOrg.parse(querySearch);
                blQuery.add(queryOrg, BooleanClause.Occur.MUST);
            } else {
                return null;
            }
            // Sort
            sort = new Sort(new SortField[]{
                        new SortField(IndexConst.PAPER_CITATIONCOUNT_FIELD, SortField.INT, true)});
            // Year
            if (last > 0) {
                Calendar calendar = GregorianCalendar.getInstance();
                calendar.setTime(new Date());
                int lastYear = calendar.get(Calendar.YEAR) - last;
                int currentYear = calendar.get(Calendar.YEAR);
                Filter filter = NumericRangeFilter.newIntRange(IndexConst.PAPER_YEAR_FIELD, lastYear, currentYear, true, true);
                topDocs = searcher.search(blQuery, filter, Integer.MAX_VALUE, sort);
            } else {
                topDocs = searcher.search(blQuery, Integer.MAX_VALUE, sort);
            }
            if (topDocs != null) {
                out = new LinkedHashMap<String, Object>();
                ArrayList<Integer> publicationList = new ArrayList<Integer>();
                int citationCount = 0;
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = searcher.doc(hit.doc);
                    citationCount += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                    publicationList.add(Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD)));
                }
                out.put("pubCount", topDocs.totalHits);
                out.put("citCount", citationCount);
                out.put("list", publicationList);
            }
            return out;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @param querySearch
     * @param last
     * @param
     * field//1:idAuthor||2:idConference||3:idJournal||4:idOrg||5:idKeyword
     * @return publicationCount, citationCount and publicationList(for H-Index
     * and G-Index) for last
     * @throws Exception
     */
    public LinkedHashMap<String, Object> getPapersForRankSubDomain(String path, String idSubdomain, String querySearch, int last, int field) throws Exception {
        try {
            IndexSearcher searcher = new IndexSearcher(getFSDirectory(path));
            LinkedHashMap<String, Object> out = null;
            BooleanQuery blQuery = new BooleanQuery();
            Sort sort = null;
            TopDocs topDocs = null;
            // querySearch
            if (field == 1) {// Author
                QueryParser parserAuthor = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDAUTHOR_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryAuthor = parserAuthor.parse(querySearch);
                blQuery.add(queryAuthor, BooleanClause.Occur.MUST);
            } else if (field == 2) {// Conference
                QueryParser parserConference = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDCONFERENCE_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryConference = parserConference.parse(querySearch);
                blQuery.add(queryConference, BooleanClause.Occur.MUST);
            } else if (field == 3) {//Journal
                QueryParser parserJournal = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_IDJOURNAL_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryJournal = parserJournal.parse(querySearch);
                blQuery.add(queryJournal, BooleanClause.Occur.MUST);
            } else if (field == 4) {// Org
                QueryParser parserOrg = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDORG_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryOrg = parserOrg.parse(querySearch);
                blQuery.add(queryOrg, BooleanClause.Occur.MUST);
            } else if (field == 5) {// Keyword
                QueryParser parserOrg = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDKEYWORD_FIELD, new StandardAnalyzer(Version.LUCENE_36));
                Query queryOrg = parserOrg.parse(querySearch);
                blQuery.add(queryOrg, BooleanClause.Occur.MUST);
            } else {
                return null;
            }
            // Subdomain
            QueryParser parserSubdomain = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query querySubdomain = parserSubdomain.parse(idSubdomain);
            blQuery.add(querySubdomain, BooleanClause.Occur.MUST);
            // Sort
            sort = new Sort(new SortField[]{
                        new SortField(IndexConst.PAPER_CITATIONCOUNT_FIELD, SortField.INT, true)});
            // Year
            if (last > 0) {
                Calendar calendar = GregorianCalendar.getInstance();
                calendar.setTime(new Date());
                int lastYear = calendar.get(Calendar.YEAR) - last;
                int currentYear = calendar.get(Calendar.YEAR);
                Filter filter = NumericRangeFilter.newIntRange(IndexConst.PAPER_YEAR_FIELD, lastYear, currentYear, true, true);
                topDocs = searcher.search(blQuery, filter, Integer.MAX_VALUE, sort);
            } else {
                topDocs = searcher.search(blQuery, Integer.MAX_VALUE, sort);
            }
            if (topDocs != null) {
                out = new LinkedHashMap<String, Object>();
                ArrayList<Integer> publicationList = new ArrayList<Integer>();
                int citationCount = 0;
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = searcher.doc(hit.doc);
                    citationCount += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                    publicationList.add(Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD)));
                }
                out.put("pubCount", topDocs.totalHits);
                out.put("citCount", citationCount);
                out.put("list", publicationList);
            }
            return out;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @return listIdAuthor
     * @throws Exception
     */
    public ArrayList<Integer> getListIdAuthorFromIdSubDomain(String path, String idSubdomain) throws Exception {
        try {
            IndexSearcher searcher = new IndexSearcher(getFSDirectory(path));
            ArrayList<Integer> listAuthor = null;
            BooleanQuery blQuery = new BooleanQuery();
            QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.AUTHOR_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query query = parser.parse(idSubdomain);
            blQuery.add(query, BooleanClause.Occur.MUST);
            TopDocs topDocs = searcher.search(blQuery, Integer.MAX_VALUE);
            if (topDocs != null) {
                listAuthor = new ArrayList<Integer>();
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = searcher.doc(hit.doc);
                    listAuthor.add(Integer.parseInt(doc.get(IndexConst.AUTHOR_IDAUTHOR_FIELD)));
                }
            }
            return listAuthor;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @return listIdConference
     * @throws Exception
     */
    public ArrayList<Integer> getListIdConferenceFromIdSubDomain(String path, String idSubdomain) throws Exception {
        try {
            IndexSearcher searcher = new IndexSearcher(getFSDirectory(path));
            ArrayList<Integer> listIdConference = null;
            BooleanQuery blQuery = new BooleanQuery();
            QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.CONFERENCE_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query query = parser.parse(idSubdomain);
            blQuery.add(query, BooleanClause.Occur.MUST);
            TopDocs topDocs = searcher.search(blQuery, Integer.MAX_VALUE);
            if (topDocs != null) {
                listIdConference = new ArrayList<Integer>();
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = searcher.doc(hit.doc);
                    listIdConference.add(Integer.parseInt(doc.get(IndexConst.CONFERENCE_IDCONFERENCE_FIELD)));
                }
            }
            return listIdConference;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @return listIdJournal
     * @throws Exception
     */
    public ArrayList<Integer> getListIdJournalFromIdSubDomain(String path, String idSubdomain) throws Exception {
        try {
            IndexSearcher searcher = new IndexSearcher(getFSDirectory(path));
            ArrayList<Integer> listIdJournal = null;
            BooleanQuery blQuery = new BooleanQuery();
            QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.JOURNAL_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query query = parser.parse(idSubdomain);
            blQuery.add(query, BooleanClause.Occur.MUST);
            TopDocs topDocs = searcher.search(blQuery, Integer.MAX_VALUE);
            if (topDocs != null) {
                listIdJournal = new ArrayList<Integer>();
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = searcher.doc(hit.doc);
                    listIdJournal.add(Integer.parseInt(doc.get(IndexConst.JOURNAL_IDJOURNAL_FIELD)));
                }
            }
            return listIdJournal;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @return listIdOrg
     */
    public ArrayList<Integer> getListIdOrgFromIdSubDomain(String path, String idSubdomain) throws Exception {
        try {
            IndexSearcher searcher = new IndexSearcher(getFSDirectory(path));
            ArrayList<Integer> listIdOrg = null;
            BooleanQuery blQuery = new BooleanQuery();
            QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.ORG_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query query = parser.parse(idSubdomain);
            blQuery.add(query, BooleanClause.Occur.MUST);
            TopDocs topDocs = searcher.search(blQuery, Integer.MAX_VALUE);
            if (topDocs != null) {
                listIdOrg = new ArrayList<Integer>();
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = searcher.doc(hit.doc);
                    listIdOrg.add(Integer.parseInt(doc.get(IndexConst.ORG_IDORG_FIELD)));
                }
            }
            return listIdOrg;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @return listIdKeyword
     */
    public ArrayList<Integer> getListIdKeywordFromIdSubDomain(String path, String idSubdomain) throws Exception {
        try {
            IndexSearcher searcher = new IndexSearcher(getFSDirectory(path));
            ArrayList<Integer> listIdKeyword = null;
            BooleanQuery blQuery = new BooleanQuery();
            QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.KEYWORD_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query query = parser.parse(idSubdomain);
            blQuery.add(query, BooleanClause.Occur.MUST);
            TopDocs topDocs = searcher.search(blQuery, Integer.MAX_VALUE);
            if (topDocs != null) {
                listIdKeyword = new ArrayList<Integer>();
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = searcher.doc(hit.doc);
                    listIdKeyword.add(Integer.parseInt(doc.get(IndexConst.KEYWORD_IDKEYWORD_FIELD)));
                }
            }
            return listIdKeyword;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    /**
     * @param path (path lucene index)
     * @param idSubdomain
     * @param last
     * @return publicationCount, citationCount for last
     */
    public LinkedHashMap<String, Integer> getPublicationsFromIdSubdomain(String path, String idSubdomain, int last) throws Exception {
        try {
            IndexSearcher searcher = new IndexSearcher(getFSDirectory(path));
            LinkedHashMap<String, Integer> out = null;
            BooleanQuery blQuery = new BooleanQuery();
            QueryParser parser = new QueryParser(Version.LUCENE_36, IndexConst.PAPER_LISTIDSUBDOMAIN_FIELD, new StandardAnalyzer(Version.LUCENE_36));
            Query query = parser.parse(idSubdomain);
            blQuery.add(query, BooleanClause.Occur.MUST);
            TopDocs topDocs = null;
            if (last > 0) {
                Calendar calendar = GregorianCalendar.getInstance();
                calendar.setTime(new Date());
                int lastYear = calendar.get(Calendar.YEAR) - last;
                int currentYear = calendar.get(Calendar.YEAR);
                Filter filter = NumericRangeFilter.newIntRange(IndexConst.PAPER_YEAR_FIELD, lastYear, currentYear, true, true);
                topDocs = searcher.search(blQuery, filter, Integer.MAX_VALUE);
            } else {
                topDocs = searcher.search(blQuery, Integer.MAX_VALUE);
            }
            if (topDocs != null) {
                out = new LinkedHashMap<String, Integer>();
                int citationCout = 0;
                ScoreDoc[] hits = topDocs.scoreDocs;
                for (int i = 0; i < topDocs.totalHits; i++) {
                    ScoreDoc hit = hits[i];
                    Document doc = searcher.doc(hit.doc);
                    citationCout += Integer.parseInt(doc.get(IndexConst.PAPER_CITATIONCOUNT_FIELD));
                }
                out.put("pubCount", topDocs.totalHits);
                out.put("citCount", citationCout);
            }
            return out;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    /**
     * calculateIndex
     *
     * @return h_index, g_index width publicationList
     * @throws Exception
     */
    public LinkedHashMap<String, Integer> getCalculateIndex(ArrayList<Integer> publicationList) throws Exception {
        LinkedHashMap<String, Integer> out = new LinkedHashMap<String, Integer>();
        int h_index;
        int g_index;
        int citationCount;
        int citationCountSum;
        // Calculate h-index for each.
        h_index = 0;
        while (h_index < publicationList.size()) {
            citationCount = publicationList.get(h_index);
            if (citationCount >= (h_index + 1)) {
                h_index++;
            } else {
                break;
            }
        }
        // Calculate g-index for each.
        g_index = 0;
        citationCountSum = 0;
        while (true) {
            if (g_index < publicationList.size()) {
                citationCountSum += publicationList.get(g_index);
            }
            if (citationCountSum >= ((g_index + 1) * (g_index + 1))) {
                g_index++;
            } else {
                break;
            }
        }
        out.put("h_index", h_index);
        out.put("g_index", g_index);
        return out;
    }

    public static FSDirectory getFSDirectory(String path) {
        FSDirectory directory = null;
        try {
            if (path != null) {
                File location = new File(path);
                directory = FSDirectory.open(location);
            }
            return directory;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }
}