/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author HuyDang
 */
public class SubdomainDTO {

    public String idSubdomain = "";
    public String subdomainName = "";
    public String idDomain = "";
    public int publicationCount = 0;
    public int citationCount = 0;
    public String listPublicationCitation = "";

    public SubdomainDTO() {
    }

    public void setIdSubdomain(String idSubdomain) {
        if (idSubdomain == null) {
            this.idSubdomain = "";
        } else {
            this.idSubdomain = idSubdomain;
        }
    }

    public void setSubdomainName(String subdomainName) {
        if (subdomainName == null) {
            this.subdomainName = "";
        } else {
            this.subdomainName = subdomainName;
        }
    }

    public void setIdDomain(String idDomain) {
        if (idDomain == null) {
            this.idDomain = "";
        } else {
            this.idDomain = idDomain;
        }
    }

    public void setPublicationCount(int publicationCount) {
        this.publicationCount = publicationCount;
    }

    public void setCitationCount(int citationCount) {
        this.citationCount = citationCount;
    }

    public void setListPublicationCitation(String listPublicationCitation) {
        if (listPublicationCitation == null) {
            this.listPublicationCitation = "";
        } else {
            this.listPublicationCitation = listPublicationCitation;
        }
    }
}