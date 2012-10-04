/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dto;

/**
 *
 * @author DucHuynh, HuyDang
 */
public class PaperDTO {

    public String abstractContent = "";
    public String authors = "";
    public String authorsName = "";
    public int citationCount = 0;
    public int referenceCount = 0;
    public String conferenceName = "";
    public String doi = "";
    public String idConference = "";
    public String idJournal = "";
    public String idPaper = "";
    public String isbn = "";
    public String journalName = "";
    public String keywordsName = "";
    public String listCitations = "";
    public String listIdAuthors = "";
    public String listIdKeywords = "";
    public String listIdOrgs = "";
    public String listIdSubdomains = "";
    public String listIdPaperCitations = "";
    public String orgsName = "";
    public String pages = "";
    public int rank = 0;
    public String title = "";
    public String volume = "";
    public String viewPublication = "";
    public int year = 0;

    public PaperDTO() {
    }

    public void setAbstractContent(String abstractContent) {
        if (abstractContent == null) {
            this.abstractContent = "";
        } else {
            this.abstractContent = abstractContent;
        }
    }

    public void setAuthors(String authors) {
        if (authors == null) {
            this.authors = "";
        } else {
            this.authors = authors;
        }
    }

    public void setAuthorsName(String authorsName) {
        if (authorsName == null) {
            this.authorsName = "";
        } else {
            this.authorsName = authorsName;
        }
    }

    public void setCitationCount(int citationCount) {
        this.citationCount = citationCount;
    }
    
    public void setReferenceCount(int referenceCount) {
        this.referenceCount = referenceCount;
    }

    public void setConferenceName(String conferenceName) {
        if (conferenceName == null) {
            this.conferenceName = "";
        } else {
            this.conferenceName = conferenceName;
        }
    }

    public void setDoi(String doi) {
        if (doi == null) {
            this.doi = "";
        } else {
            this.doi = doi;
        }
    }

    public void setKeywordsName(String keywordsName) {
        if (keywordsName == null) {
            this.keywordsName = "";
        } else {
            this.keywordsName = keywordsName;
        }
    }

    public void setOrgsName(String orgsName) {
        if (orgsName == null) {
            this.orgsName = "";
        } else {
            this.orgsName = orgsName;
        }
    }

    public void setIdConference(String idConference) {
        if (idConference == null) {
            this.idConference = "";
        } else {
            this.idConference = idConference;
        }
    }

    public void setIdJournal(String idJournal) {
        if (idJournal == null) {
            this.idJournal = "";
        } else {
            this.idJournal = idJournal;
        }
    }

    public void setIdPaper(String idPaper) {
        if (idPaper == null) {
            this.idPaper = "";
        } else {
            this.idPaper = idPaper;
        }
    }

    public void setIsbn(String isbn) {
        if (isbn == null) {
            this.isbn = "";
        } else {
            this.isbn = isbn;
        }
    }

    public void setJournalName(String journalName) {
        if (journalName == null) {
            this.journalName = "";
        } else {
            this.journalName = journalName;
        }
    }

    public void setListCitations(String listCitations) {
        if (listCitations == null) {
            this.listCitations = "";
        } else {
            this.listCitations = listCitations;
        }
    }

    public void setListIdAuthors(String listIdAuthors) {
        if (listIdAuthors == null) {
            this.listIdAuthors = "";
        } else {
            this.listIdAuthors = listIdAuthors;
        }
    }

    public void setListIdKeywords(String listIdKeywords) {
        if (listIdKeywords == null) {
            this.listIdKeywords = "";
        } else {
            this.listIdKeywords = listIdKeywords;
        }
    }

    public void setListIdOrgs(String listIdOrgs) {
        if (listIdOrgs == null) {
            this.listIdOrgs = "";
        } else {
            this.listIdOrgs = listIdOrgs;
        }
    }

    public void setListIdSubdomains(String listIdSubdomains) {
        if (listIdSubdomains == null) {
            this.listIdSubdomains = "";
        } else {
            this.listIdSubdomains = listIdSubdomains;
        }
    }

    public void setListIdPaperCitations(String listIdPaperCitations) {
        if (listIdPaperCitations == null) {
            this.listIdPaperCitations = "";
        } else {
            this.listIdPaperCitations = listIdPaperCitations;
        }
    }

    public void setPages(String pages) {
        if (pages == null) {
            this.pages = "";
        } else {
            this.pages = pages;
        }
    }

    public void setTitle(String title) {
        if (title == null) {
            this.title = "";
        } else {
            this.title = title;
        }
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public void setVolume(String volume) {
        if (volume == null) {
            this.volume = "";
        } else {
            this.volume = volume;
        }
    }

    public void setViewPublication(String viewPublication) {
        if (viewPublication == null) {
            this.viewPublication = "";
        } else {
            this.viewPublication = viewPublication;
        }
    }

    public void setYear(int year) {
        this.year = year;
    }
}