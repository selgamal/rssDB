-- Script to create RSS sqlite database

-- DROP TABLE IF EXISTS feedsInfo;
-- DROP TABLE IF EXISTS filingsInfo;
-- DROP TABLE IF EXISTS rssItems;
-- DROP TABLE IF EXISTS filersInfo;
-- DROP TABLE IF EXISTS filesInfo;
-- DROP TABLE IF EXISTS cikTickerMapping;
-- DROP TABLE IF EXISTS industry;
-- DROP TABLE IF EXISTS industry_level;
-- DROP TABLE IF EXISTS industry_structure;
-- DROP VIEW IF EXISTS v_count_filings_by_feed;
-- DROP VIEW IF EXISTS v_duplicate_filings;
-- DROP VIEW IF EXISTS v_filingsSummary;

CREATE TABLE IF NOT EXISTS feedsInfo (
    feedId INTEGER NOT NULL UNIQUE PRIMARY KEY,
    feedMonth TEXT NOT NULL,
    title TEXT,
    link TEXT,
    feedLink TEXT NOT NULL,
    description TEXT,
    language TEXT,
    pubDate TEXT,
    lastBuildDate TEXT,
    lastModifiedDate TEXT
);

CREATE TABLE IF NOT EXISTS filingsInfo (
    filingId INTEGER NOT NULL UNIQUE PRIMARY KEY,
    feedId INTEGER NOT NULL,
    filingLink TEXT,
    entryPoint TEXT,
    enclosureUrl TEXT,
    enclosureSize INTEGER,
    pubDate TEXT,
    companyName TEXT,
    formType TEXT,
    inlineXBRL INTEGER DEFAULT 0,
    filingDate TEXT,
    cikNumber TEXT,
    accessionNumber TEXT,
    fileNumber TEXT,
    acceptanceDatetime TEXT,
    period TEXT,
    assignedSic INTEGER DEFAULT 0,
    assistantDirector TEXT, 
    fiscalYearEnd TEXT,
    fiscalYearEndMonth INTEGER,
    fiscalYearEndDay INTEGER,
    duplicate INTEGER DEFAULT 0,
    FOREIGN KEY (feedId) 
        REFERENCES feedsInfo (feedId)
            ON UPDATE RESTRICT 
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "filesInfo" (
    "fileId" INTEGER NOT NULL UNIQUE PRIMARY KEY,
    "filingId" INTEGER,
    "feedId" INTEGER,
    "accessionNumber" TEXT,
    "sequence" INTEGER,
    "file" TEXT,
    "type" TEXT,
    "size" INTEGER,
    "description" TEXT,
    "inlineXBRL" INTEGER DEFAULT 0,
    "url" TEXT,
    type_tag TEXT DEFAULT 0,
    duplicate INTEGER DEFAULT 0,
    FOREIGN KEY (filingId) 
        REFERENCES filingsInfo (filingId)
            ON UPDATE RESTRICT 
            ON DELETE CASCADE
);

CREATE INDEX "filesInfo_filingID_indx" ON "filesInfo" (
	"filingId"	ASC
);


CREATE TABLE IF NOT EXISTS rssItems (
	filingId INTEGER NOT NULL UNIQUE PRIMARY KEY,
	rssItem BLOB,
	FOREIGN KEY (filingId) 
        REFERENCES filingsInfo (filingId)
            ON UPDATE RESTRICT 
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "filersInfo" (
    "cikNumber" TEXT NOT NULL UNIQUE PRIMARY KEY, 
    "formerNames" TEXT,
    "industry_code" integer,
    "industry_description" TEXT,
    "stateOfIncorporation" TEXT, 
    "mailingState" TEXT,
    "mailingCity" TEXT,
    "mailingZip" TEXT, 
    "conformedName" TEXT, 
    "businessCity" TEXT,  
    "businessState" TEXT, 
    "businessZip" TEXT,
    "country" TEXT
);

CREATE TABLE IF NOT EXISTS "cikTickerMapping" (
    "cikNumber" TEXT,
    "tickerSymbol" TEXT
);

CREATE TABLE IF NOT EXISTS "locations" (
    "code" TEXT NOT NULL UNIQUE PRIMARY KEY,
    "latitude" REAL,
    "longitude" REAL,
    "country" TEXT,
    "stateProvince" TEXT,
    "locationFix" TEXT
);

CREATE TABLE IF NOT EXISTS "lastUpdate" (
    "id" integer NOT NULL UNIQUE PRIMARY KEY,
    "lastUpdate" TEXT
);

INSERT INTO "lastUpdate" ("id", "lastUpdate") VALUES (0, datetime(0, 'unixepoch'));

CREATE TABLE IF NOT EXISTS "formulae" (
    "formulaId" INTEGER NOT NULL UNIQUE PRIMARY KEY,
    "fileName" TEXT NOT NULL,
    "description" TEXT,
    "formulaLinkbase" BLOB,
    "dateTimeAdded" TEXT DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS "formulaeResults" (
    "filingId" INTEGER NOT NULL,
    "formulaId" INTEGER NOT NULL,
    "inlineXBRL" INTEGER NOT NULL DEFAULT 0,
    "formulaOutput" BLOB NOT NULL,
    "assertionsResults" BLOB,
    "dateTimeProcessed" TEXT,
    "processingLog" BLOB,
    UNIQUE ("filingId", "formulaId"),
    FOREIGN KEY (filingId) 
    REFERENCES filingsInfo (filingId)
        ON UPDATE RESTRICT 
        ON DELETE CASCADE
    FOREIGN KEY (formulaId) 
    REFERENCES formulae (formulaId)
        ON UPDATE RESTRICT 
        ON DELETE CASCADE
);


CREATE VIEW IF NOT EXISTS v_duplicate_filings
 AS
 WITH x AS (
         SELECT "accessionNumber",
            min("filingId") AS "filingId",
            count("filingId") AS num
           FROM "filingsInfo" WHERE "duplicate" = 0
          GROUP BY "accessionNumber"
        )
 SELECT x."filingId"
   FROM x
  WHERE x.num > 1;

CREATE VIEW IF NOT EXISTS "v_filingsSummary"
 AS
select "feedId", "formType", "assignedSic", "inlineXBRL", count("filingId") as "count"
from "filingsInfo"
where "duplicate" = 0
group by "feedId", "formType", "assignedSic", "inlineXBRL"
order  by "feedId" desc;

-- from files in Arelle\plugin\xbrlDB\sql\semantic\

CREATE TABLE IF NOT EXISTS industry (
    industry_id INTEGER,
    industry_classification TEXT,
    industry_code integer,
    industry_description TEXT,
    depth integer,
    parent_id INTEGER,
    PRIMARY KEY (industry_id)
);

CREATE TABLE IF NOT EXISTS industry_level (
    industry_level_id INTEGER,
    industry_classification TEXT,
    ancestor_id INTEGER,
    ancestor_code INTEGER,
    ancestor_depth INTEGER,
    descendant_id INTEGER,
    descendant_code INTEGER,
    descendant_depth INTEGER,
    PRIMARY KEY (industry_level_id)
);

CREATE TABLE IF NOT EXISTS industry_structure (
    industry_structure_id INTEGER,
    industry_classification TEXT NOT NULL,
    depth INTEGER NOT NULL,
    level_name TEXT,
    PRIMARY KEY (industry_structure_id)
);

CREATE VIEW v_count_filings_by_feed
 AS
 WITH fd AS (
         SELECT "feedsInfo"."feedId",
            "feedsInfo"."feedMonth",
            "feedsInfo"."feedLink",
            "feedsInfo"."lastModifiedDate"
           FROM "feedsInfo"
        ), counts AS (
         SELECT "filingsInfo"."feedId",
            count(*) AS count
           FROM "filingsInfo"
          GROUP BY "filingsInfo"."feedId"
        )
 SELECT fd."feedId",
    fd."feedMonth",
    fd."feedLink",
    fd."lastModifiedDate",
    counts.count
   FROM fd
     LEFT JOIN counts USING ("feedId")
  ORDER BY fd."feedId";