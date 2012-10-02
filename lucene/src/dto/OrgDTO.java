/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author HuyDang
 */
public class OrgDTO {

    public String idOrg = "";
    public String orgName = "";
    public String website = "";
    public String continent = "";
    public String idOrgParent = "";
    public int h_index = 0;
    public int g_index = 0;
    public int publicationCount = 0;
    public int citationCount = 0;
    public String listIdSubdomains = "";
    public String listPublicationCitation = "";

    public OrgDTO() {
    }

    public void setIdOrg(String idOrg) {
        if (idOrg == null) {
            this.idOrg = "";
        } else {
            this.idOrg = idOrg;
        }
    }

    public void setOrgName(String orgName) {
        if (orgName == null) {
            this.orgName = "";
        } else {
            this.orgName = orgName;
        }
    }

    public void setWebsite(String website) {
        if (website == null) {
            this.website = "";
        } else {
            this.website = website;
        }
    }

    public void setContinent(String continent) {
        if (continent == null) {
            this.continent = "";
        } else {
            this.continent = continent;
        }
    }

    public void setIdOrgParent(String idOrgParent) {
        if (idOrgParent == null) {
            this.idOrgParent = "";
        } else {
            this.idOrgParent = idOrgParent;
        }
    }

    public void setH_Index(int h_index) {
        this.h_index = h_index;
    }

    public void setG_Index(int g_index) {
        this.g_index = g_index;
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