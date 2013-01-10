/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import constant.ConnectionPool;
import database.PaperPaperTB;
import database.RankPaperTB;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

/**
 *
 * @author HuyDang
 */
public class _Rank_Paper {

    public _Rank_Paper() {
    }

    /**
     *
     * @param connectionPool Kết nối csdl
     * @return số lượng record trong bảng rank_paper mới tạo, và thời gian thực
     * hiện bảng rank
     */
    public String _run(ConnectionPool connectionPool) {
        String out = "";
        try {
            long start = new Date().getTime();
            int count = this._rank(connectionPool);
            long end = new Date().getTime();
            out = "Count: " + count + " record - Time index :" + (end - start) + " milisecond";
        } catch (Exception ex) {
            out = ex.getMessage();
        }
        return out;
    }

    /**
     *
     * @param connectionPool
     * @Summary: Tạo bảng trong csdl(xóa bảng cũ nếu đã tồn tại), loại bỏ các
     * record có idpaper trùng với idpaperRef Insert các record từ bảng
     * paper_paper sang bảng rank, với chỉ số rank tăng dần
     */
    public int _rank(ConnectionPool connectionPool) throws SQLException {
        int count = 0;
        Connection connection = connectionPool.getConnection();
        try {
            Statement statement = connection.createStatement();
            // Drop
            String drop = "DROP TABLE IF EXISTS `" + RankPaperTB.TABLE_NAME + "`;";
            statement.execute(drop);
            // Create
            String create = "CREATE TABLE IF NOT EXISTS `" + RankPaperTB.TABLE_NAME + "` (`" + RankPaperTB.COLUMN_PAPERID + "` INT(10) NOT NULL, `" + RankPaperTB.COLUMN_CITATIONCOUNT + "` INT(10) NULL, `" + RankPaperTB.COLUMN_RANK + "` INT(10) NULL AUTO_INCREMENT, PRIMARY KEY (`" + RankPaperTB.COLUMN_PAPERID + "`), INDEX `index2` (`" + RankPaperTB.COLUMN_RANK + "` ASC)) ENGINE = MyISAM DEFAULT CHARACTER SET = utf8;";
            statement.execute(create);
            // Clean up data
            String detele = "DELETE FROM " + PaperPaperTB.TABLE_NAME + " WHERE " + PaperPaperTB.COLUMN_PAPERID + " = " + PaperPaperTB.COLUMN_PAPERREFID + ";";
            statement.execute(detele);
            // Truncate all old ranking data
            String truncate = "TRUNCATE TABLE `" + RankPaperTB.TABLE_NAME + "`;";
            statement.execute(truncate);
            // -- Insert count data
            // 1. TABLE _rank_paper, citationCount: all paper cite to this paper
            String insert = "INSERT IGNORE INTO " + RankPaperTB.TABLE_NAME + "(" + RankPaperTB.COLUMN_PAPERID + ", " + RankPaperTB.COLUMN_CITATIONCOUNT + ") SELECT pp." + PaperPaperTB.COLUMN_PAPERREFID + ", COUNT(DISTINCT pp." + PaperPaperTB.COLUMN_PAPERID + ") FROM " + PaperPaperTB.TABLE_NAME + " pp GROUP BY pp." + PaperPaperTB.COLUMN_PAPERREFID + ";";
            count = statement.executeUpdate(insert);

            statement.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            connection.close();
        }
        return count;
    }

    /**
     * hàm chạy thử
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
            ConnectionPool connectionPool = new ConnectionPool(user, pass, database, port);
            _Rank_Paper indexer = new _Rank_Paper();
            System.out.println(indexer._run(connectionPool));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}