-- Script to create RSS database in postgresql

-- DROP SCHEMA IF EXISTS "rssFeeds" CASCADE;
-- CREATE SCHEMA "rssFeeds";
-- SET search_path = "rssFeeds";


-- DROP TABLE IF EXISTS "feedsInfo" CASCADE;
-- DROP TABLE IF EXISTS "filingsInfo" CASCADE;
-- DROP TABLE IF EXISTS "rssItems" CASCADE;
-- DROP TABLE IF EXISTS "filersInfo" CASCADE;
-- DROP TABLE IF EXISTS "filesInfo" CASCADE;
-- DROP TABLE IF EXISTS "cikTickerMapping" CASCADE;
-- DROP TABLE IF EXISTS industry CASCADE;
-- DROP TABLE IF EXISTS industry_level CASCADE;
-- DROP TABLE IF EXISTS industry_structure CASCADE;
-- DROP TABLE IF EXISTS "formulae" CASCADE;
-- DROP TABLE IF EXISTS "formulaeResults" CASCADE;
-- DROP TABLE IF EXISTS "lastUpdate" CASCADE;
-- DROP TABLE IF EXISTS "locations" CASCADE;
-- DROP FUNCTION IF EXISTS row_count_all CASCADE;
-- DROP VIEW IF EXISTS v_count_filings_by_feed;
-- DROP VIEW IF EXISTS v_duplicate_filings;
-- DROP VIEW IF EXISTS v_filingsSummary;


CREATE TABLE IF NOT EXISTS "feedsInfo" (
    "feedId" INTEGER NOT NULL UNIQUE PRIMARY KEY,
    "feedMonth" DATE NOT NULL,
    title TEXT,
    link TEXT,
    "feedLink" TEXT NOT NULL,
    description TEXT,
    language TEXT,
    "pubDate" TIMESTAMP WITH TIME ZONE,
    "lastBuildDate" TIMESTAMP WITH TIME ZONE,
	"lastModifiedDate" TIMESTAMP WITHOUT TIME ZONE
);

CREATE INDEX "feedsInfo_idx02" ON "feedsInfo" USING btree ("lastModifiedDate"); 

CREATE TABLE IF NOT EXISTS "filingsInfo" (
    "filingId" BIGINT NOT NULL UNIQUE PRIMARY KEY,
    "feedId" INTEGER NOT NULL,
    "filingLink" TEXT,
    "entryPoint" TEXT,
    "enclosureUrl" TEXT,
    "enclosureSize" BIGINT, 
    "pubDate" TIMESTAMP WITHOUT TIME ZONE,
    "companyName" TEXT,
    "formType" TEXT,
    "inlineXBRL" INTEGER DEFAULT 0,
    "filingDate" TIMESTAMP WITHOUT TIME ZONE,
    "cikNumber" TEXT,
    "accessionNumber" TEXT,
    "fileNumber" TEXT,
    "acceptanceDatetime" TIMESTAMP WITHOUT TIME ZONE,
    period DATE,
    "assignedSic" INTEGER DEFAULT 0,
    "assistantDirector" Text,
    "fiscalYearEnd" TEXT,
    "fiscalYearEndMonth" INTEGER,
    "fiscalYearEndDay" INTEGER,
    "duplicate" INTEGER DEFAULT 0,
    FOREIGN KEY ("feedId") 
        REFERENCES "feedsInfo" ("feedId")
            ON UPDATE RESTRICT 
            ON DELETE CASCADE
);

CREATE INDEX "filingsInfo_idx02" ON "filingsInfo" USING btree ("accessionNumber"); 
CREATE INDEX "filingsInfo_idx03" ON "filingsInfo" USING btree ("formType"); 
CREATE INDEX "filingsInfo_idx04" ON "filingsInfo" USING btree ("assignedSic"); 
CREATE INDEX "filingsInfo_idx05" ON "filingsInfo" USING btree ("filingDate"); 
CREATE INDEX "filingsInfo_idx06" ON "filingsInfo" USING btree ("acceptanceDatetime"); 
CREATE INDEX "filingsInfo_idx07" ON "filingsInfo" USING btree ("duplicate"); 
CREATE INDEX "filingsInfo_idx08" ON "filingsInfo" USING btree ("inlineXBRL"); 

CREATE TABLE IF NOT EXISTS "filesInfo" (
    "fileId" BIGINT NOT NULL UNIQUE PRIMARY KEY,
    "filingId" BIGINT,
    "feedId" INTEGER,
    "accessionNumber" TEXT,
    "sequence" INTEGER,
    "file" TEXT,
    "type" TEXT,
    "size" BIGINT,
    "description" TEXT,
    "inlineXBRL" INTEGER DEFAULT 0,
    "url" TEXT,
    type_tag TEXT DEFAULT 0,
    "duplicate" INTEGER DEFAULT 0,
    FOREIGN KEY ("filingId") 
    REFERENCES "filingsInfo" ("filingId")
        ON UPDATE RESTRICT 
        ON DELETE CASCADE
);

CREATE INDEX "filesInfo_idx02" ON "filesInfo" USING btree ("filingId");
CREATE INDEX "filesInfo_idx03" ON "filesInfo" USING btree ("duplicate");
CREATE INDEX "filesInfo_idx04" ON "filesInfo" USING btree ("type_tag");

CREATE TABLE IF NOT EXISTS "rssItems" (
	"filingId" BIGINT NOT NULL UNIQUE PRIMARY KEY,
	"rssItem" XML,
	FOREIGN KEY ("filingId") 
        REFERENCES "filingsInfo" ("filingId")
            ON UPDATE RESTRICT 
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "filersInfo" (
    "cikNumber" TEXT NOT NULL UNIQUE PRIMARY KEY, 
    "formerNames" JSON,
    "industry_code" integer,
    "industry_description" Text,
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

CREATE INDEX "filersInfo_idx02" ON "filersInfo" USING btree ("cikNumber");
CREATE INDEX "filersInfo_idx03" ON "filersInfo" USING btree ("industry_code");

CREATE TABLE IF NOT EXISTS "cikTickerMapping" (
    "cikNumber" TEXT,
    "tickerSymbol" TEXT
);
CREATE INDEX "cikTickerMapping_idx01" ON "cikTickerMapping" USING btree ("cikNumber");

CREATE TABLE IF NOT EXISTS "locations" (
    "code" TEXT NOT NULL UNIQUE PRIMARY KEY,
    "latitude" NUMERIC,
    "longitude" NUMERIC,
    "country" TEXT,
    "stateProvince" TEXT,
    "locationFix" TEXT
);

CREATE TABLE IF NOT EXISTS "lastUpdate" (
    "id" NUMERIC NOT NULL UNIQUE PRIMARY KEY,
    "lastUpdate" TIMESTAMP WITHOUT TIME ZONE
);

INSERT INTO "lastUpdate" ("id", "lastUpdate") VALUES (0, date_trunc('second',to_timestamp(0)::timestamp));


CREATE TABLE IF NOT EXISTS "formulae" (
    "formulaId" INTEGER NOT NULL UNIQUE PRIMARY KEY,
    "fileName" TEXT NOT NULL,
    "description" TEXT,
    "formulaLinkbase" XML,
    "dateTimeAdded" TIMESTAMP WITHOUT TIME ZONE
);


CREATE TABLE IF NOT EXISTS "formulaeResults" (
    "filingId" BIGINT NOT NULL,
    "formulaId" BIGINT NOT NULL,
    "inlineXBRL" INTEGER DEFAULT 0,
    "formulaOutput" XML,
    "assertionsResults" JSON,
    "dateTimeProcessed" TIMESTAMP WITHOUT TIME ZONE,
    "processingLog" XML,
    UNIQUE ("filingId", "formulaId"),
    FOREIGN KEY ("filingId") 
    REFERENCES "filingsInfo" ("filingId")
        ON UPDATE RESTRICT 
        ON DELETE CASCADE,
    FOREIGN KEY ("formulaId")
    REFERENCES "formulae" ("formulaId")
        ON UPDATE RESTRICT 
        ON DELETE CASCADE
);


CREATE VIEW "v_duplicate_filings"
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

CREATE VIEW "v_filingsSummary"
 AS
select "feedId", "formType", "assignedSic", "inlineXBRL", count("filingId") as "count"
from "filingsInfo"
where "duplicate" = 0
group by "feedId", "formType", "assignedSic", "inlineXBRL"
order  by "feedId" desc;


-- from files in Arelle\plugin\xbrlDB\sql\semantic\

CREATE TABLE IF NOT EXISTS industry (
    industry_id bigint NOT NULL,
    industry_classification character varying,
    industry_code integer,
    industry_description character varying,
    depth integer,
    parent_id bigint,
    PRIMARY KEY (industry_id)
);

CREATE TABLE IF NOT EXISTS industry_level (
    industry_level_id bigint NOT NULL,
    industry_classification character varying,
    ancestor_id bigint,
    ancestor_code integer,
    ancestor_depth integer,
    descendant_id bigint,
    descendant_code integer,
    descendant_depth integer,
    PRIMARY KEY (industry_level_id)
);

CREATE TABLE IF NOT EXISTS industry_structure (
    industry_structure_id bigint NOT NULL,
    industry_classification character varying NOT NULL,
    depth integer NOT NULL,
    level_name character varying,
    PRIMARY KEY (industry_structure_id)
);

CREATE FUNCTION row_count_all(
	schema_name text DEFAULT NULL::text)
    RETURNS TABLE(table_name text, row_count bigint) 
    LANGUAGE 'plpgsql'

AS $BODY$

DECLARE
 table_name text;
BEGIN
  IF
    schema_name is NULL
  THEN
    SELECT current_schema INTO schema_name;
  END IF;
	FOR table_name in SELECT tablename
	FROM pg_catalog.pg_tables
	WHERE schemaname = schema_name
  LOOP
    RETURN QUERY EXECUTE format('select cast(%L as text),count(*) from %I.%I',
       table_name, schema_name, table_name);
  END LOOP;
END
$BODY$;

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