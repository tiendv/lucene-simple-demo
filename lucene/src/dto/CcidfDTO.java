/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author HuyDang
 */
public class CcidfDTO {

    public String idPaper = "";
    public int idRelatedPaper = 0;
    public double weight = 0;

    public CcidfDTO() {
    }

    public void setIdPaper(String idPaper) {
        if (idPaper == null) {
            this.idPaper = "";
        } else {
            this.idPaper = idPaper;
        }
    }

    public void setIdRelatedPaper(int idRelatedPaper) {
        this.idRelatedPaper = idRelatedPaper;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }
}