/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author Administrator
 */
public class PubCiDTO {

    private int publication;
    private int citation;
    private int year;

    public PubCiDTO() {
    }

    public PubCiDTO(int p, int c, int y) {
        this.publication = p;
        this.citation = c;
        this.year = y;
    }

    public void setPublication(int p) {
        this.publication = p;
    }

    public void setCitation(int c) {
        this.citation = c;
    }

    public void setYear(int y) {
        this.year = y;
    }

    public int getPublication() {
        return this.publication;
    }

    public int getCitation() {
        return this.citation;
    }

    public int getYear() {
        return this.year;
    }
}