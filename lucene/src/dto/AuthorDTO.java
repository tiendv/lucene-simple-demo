/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author HuyDang
 */
public class AuthorDTO {

    public String idAuthor = "";
    public String authorName = "";
    public String image = "";
    public String website = "";
    public String idOrg = "";
    public int h_index = 0;
    public int g_index = 0;
    public int publicationCount = 0;
    public int citationCount = 0;
    public int coAuthorCount = 0;
    public int rank = 0;
    public String listIdSubdomains = "";
    public String listPublicationCitation = "";
    public String listRankSubdomain = ""; // Arraylist{idSubdomain, publicationCount, citationCount, rank, coAuthorCount, h_index, g_index}

    public AuthorDTO() {
    }

    public void setIdAuthor(String idAuthor) {
        if (idAuthor == null) {
            this.idAuthor = "";
        } else {
            this.idAuthor = idAuthor;
        }
    }

    public void setAuthorName(String authorName) {
        if (authorName == null) {
            this.authorName = "";
        } else {
            this.authorName = authorName;
        }
    }

    public void setImage(String image) {
        if (image == null) {
            this.image = "";
        } else {
            this.image = image;
        }
    }

    public void setWebsite(String website) {
        if (website == null) {
            this.website = "";
        } else {
            this.website = website;
        }
    }

    public void setIdOrg(String idOrg) {
        if (idOrg == null) {
            this.idOrg = "";
        } else {
            this.idOrg = idOrg;
        }
    }

    public void setH_Index(int h_index) {
        this.h_index = h_index;
    }

    public void setG_Index(int g_index) {
        this.g_index = g_index;
    }

    public void setCoAuthorCount(int coAuthorCount) {
        this.coAuthorCount = coAuthorCount;
    }

    public void setRank(int rank) {
        this.rank = rank;
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

    public void setListRankSubdomain(String listRankSubdomain) {
        if (listRankSubdomain == null) {
            this.listRankSubdomain = "";
        } else {
            this.listRankSubdomain = listRankSubdomain;
        }
    }
}