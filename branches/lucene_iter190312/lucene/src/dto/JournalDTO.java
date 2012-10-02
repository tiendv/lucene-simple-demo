/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author HuyDang
 */
public class JournalDTO {

    public String idJournal = "";
    public String journalName = "";
    public String website = "";
    public String organization = "";
    public int yearStart = 0;
    public int yearEnd = 0;
    public int g_index = 0;
    public int publicationCount = 0;
    public int citationCount = 0;
    public String listIdSubdomains = "";
    public String listPublicationCitation = "";

    public JournalDTO() {
    }

    public void setIdJournal(String idJournal) {
        if (idJournal == null) {
            this.idJournal = "";
        } else {
            this.idJournal = idJournal;
        }
    }

    public void setJournalName(String journalName) {
        if (journalName == null) {
            this.journalName = "";
        } else {
            this.journalName = journalName;
        }
    }

    public void setWebsite(String website) {
        if (website == null) {
            this.website = "";
        } else {
            this.website = website;
        }
    }

    public void setOrganization(String organization) {
        if (organization == null) {
            this.organization = "";
        } else {
            this.organization = organization;
        }
    }

    public void setYearStart(int yearStart) {
        this.yearStart = yearStart;
    }

    public void setYearEnd(int yearEnd) {
        this.yearEnd = yearEnd;
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
