/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author DaoLV
 */
public class RSSEdgeDTO {

    public String idEdge = "";
    public float weight = 0;

    public RSSEdgeDTO() {
    }

    public void setIdEdge(String authorID1, String authorID2) {
        this.idEdge = authorID1 + "_" + authorID2;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }
}