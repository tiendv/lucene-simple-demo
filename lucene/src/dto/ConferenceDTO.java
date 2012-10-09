/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author HuyDang
 */
public class ConferenceDTO {

    public String idConference = "";
    public String conferenceName = "";
    public String website = "";
    public String organization = "";
    public String organizedLocation = "";
    public String duration = "";
    public int yearStart = 0;
    public int yearEnd = 0;
    public int h_index = 0;
    public int g_index = 0;
    public int publicationCount = 0;
    public int citationCount = 0;
    public String listIdSubdomain = "";
    public String listPublicationCitation = "";

    public ConferenceDTO() {
    }

    public void setIdConference(String idConference) {
        if (idConference == null) {
            this.idConference = "";
        } else {
            this.idConference = idConference;
        }
    }

    public void setConferenceName(String conferenceName) {
        if (conferenceName == null) {
            this.conferenceName = "";
        } else {
            this.conferenceName = conferenceName;
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

    public void setOrganizedLocation(String organizedLocation) {
        if (organizedLocation == null) {
            this.organizedLocation = "";
        } else {
            this.organizedLocation = organizedLocation;
        }
    }

    public void setDuration(String duration) {
        if (duration == null) {
            this.duration = "";
        } else {
            this.duration = duration;
        }
    }

    public void setYearStart(int yearStart) {
        this.yearStart = yearStart;
    }

    public void setYearEnd(int yearEnd) {
        this.yearEnd = yearEnd;
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

    public void setListIdSubdomain(String listIdSubdomain) {
        if (listIdSubdomain == null) {
            this.listIdSubdomain = "";
        } else {
            this.listIdSubdomain = listIdSubdomain;
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
