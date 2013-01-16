/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.IndexConst;
import dto.RTBVSEdgeDTO;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Date;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author DaoLV
 */
public class RTBVSGraphIndexer {

    private String path = "D:\\DaoLVData\\INDEX\\";

    public RTBVSGraphIndexer(String path) {
        try {
            this.path = path;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
    
    public String _run(String filePath) {
        String out = "";
        try {
            File indexDir = new File(path + IndexConst.RTBVSGRAPH_DIRECTORY_PATH);
            long start = new Date().getTime();
            int count = this._index(filePath, indexDir);
            long end = new Date().getTime();
            out = "Index : " + count + " files : Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }
    
    private int _index(String filePath, File indexDir) {
        int count = 0;
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
            Directory directory = FSDirectory.open(indexDir);
            IndexWriter writer = new IndexWriter(directory, config);
            
            FileInputStream fos = new FileInputStream(filePath);
            Reader r = new InputStreamReader(fos,"UTF8");
            BufferedReader reader = new BufferedReader(r);
            String line = null;
            reader.readLine(); // ignore first row
            String[] tokens;
            String authorID1 ;
            String authorID2 ;
            float weight;
            RTBVSEdgeDTO edgeDTO = null;
            while((line=reader.readLine()) != null)
            {
                tokens = line.split("\t");
                authorID1 = tokens[0];
                authorID2 = tokens[1];
                weight = Float.parseFloat(tokens[2]);
                edgeDTO = new RTBVSEdgeDTO();
                edgeDTO.setIdEdge(authorID1,authorID2);
                edgeDTO.setWeight(weight);
                
                Document d = new Document();
                d.add(new Field(IndexConst.RTBVSGRAPH_IDEDGE_FIELD, edgeDTO.idEdge, Field.Store.YES, Field.Index.ANALYZED));
                d.add(new NumericField(IndexConst.RBTVSGRAPH_WEIGHT_FIELD, Field.Store.YES, false).setFloatValue(edgeDTO.weight));
                writer.addDocument(d);
                System.out.println("Indexing : " + count++ + "\t");
                d = null;
                edgeDTO = null;
            }
            count = writer.numDocs();
            writer.optimize();
            writer.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return 0;
        }
        return count;
    }

    public static void main(String args[]) {
        // TODO add your handling code here:
        try {
            String path = "D:\\DaoLVData\\INDEX\\";
            RTBVSGraphIndexer indexer = new RTBVSGraphIndexer(path);
            String filePath = "D:\\DaoLVData\\RTBVSGraphData\\RTBVSGraph.txt";
            System.out.println(indexer._run(filePath));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}