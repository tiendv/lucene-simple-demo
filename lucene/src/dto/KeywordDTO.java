/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author HuyDang
 */
public class KeywordDTO {

    public String idKeyword = "";
    public String keyword = "";
    public String stemmingVariations = "";
    public int publicationCount = 0;
    public int citationCount = 0;
    public String listIdSubdomains = "";
    public String listPublicationCitation = "";

    public KeywordDTO() {
    }

    public void setIdKeyword(String idKeyword) {
        if (idKeyword == null) {
            this.idKeyword = "";
        } else {
            this.idKeyword = idKeyword;
        }
    }

    public void setKeyword(String keyword) {
        if (keyword == null) {
            this.keyword = "";
        } else {
            this.keyword = keyword;
        }
    }

    public void setStemmingVariations(String stemmingVariations) {
        if (stemmingVariations == null) {
            this.stemmingVariations = "";
        } else {
            this.stemmingVariations = stemmingVariations;
        }
    }

    public void setPublicationCount(int publicationCount) {
        this.publicationCount = publicationCount;
    }

    public void setCitationCount(int citationCount) {
        this.citationCount = citationCount;
    }

    public void setListIdSubdomains(String listIdSubdomains) {
        if (listIdSubdomains == null) {
            this.listIdSubdomains = "";
        } else {
            this.listIdSubdomains = listIdSubdomains;
        }
    }

    public void setListPublicationCitation(String listPublicationCitation) {
        if (listPublicationCitation == null) {
            this.listPublicationCitation = "";
        } else {
            this.listPublicationCitation = listPublicationCitation;
        }
    }
}