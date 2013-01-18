/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author DaoLV
 */
public class CoAuthorEdgeDTO {

    public String idAuthor1;
    public String listIdAuthor2="";

    public CoAuthorEdgeDTO() {
    }

    public void setIdAuthor1(String idAuthor1) {
        this.idAuthor1 = idAuthor1;
    }

    public void setListIdAuthor2(String listIdAuthor2) {
        this.listIdAuthor2 = listIdAuthor2;
    }
    
    public void addIdAuthor2(String idAuthor2)
    {
        if (listIdAuthor2.isEmpty())
            listIdAuthor2+=idAuthor2;
        else
            listIdAuthor2+= ";"+idAuthor2;
    }
}