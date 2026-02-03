/* Refined Relational Model 

Publications(PID, pubkey, subclass, title, booktitle, year, month, publisher,
			 journal, CID -> Publications)
Authors(AID, name)
Authored(PID -> Publications, AID -> Authors)

*/



/* Schema Creation


Create schema `pubschema` with relations below
- `Publications`
- `Authors`
- `Authored`

And additional 2 temp tables that is located in `pubschema_staging`:
- `rawData`
- `rawAuthors`

`Publications` and `Authors` table will have generated serial PK for performance purposes
Foreign key if exists, will be defined after bulk data insertions
Both temp tables is deleted after data processing

*/

CREATE SCHEMA IF NOT EXISTS pubschema;
CREATE SCHEMA IF NOT EXISTS pubschema_staging;

CREATE TABLE IF NOT EXISTS pubschema_staging.rawData(
	type 		TEXT,
	key 		TEXT,
	mdate 		TEXT,
	publtype 	TEXT,
	reviewid 	TEXT,
	rating 		TEXT,
	cdate 		TEXT,
	author 		TEXT,
	editor 		TEXT,
	title 		TEXT,
	booktitle 	TEXT,
	pages 		TEXT,
	year 		TEXT,
	address 	TEXT,
	journal 	TEXT,
	volume 		TEXT,
	number 		TEXT,
	month 		TEXT,
	url 		TEXT,
	ee 			TEXT,
	cdrom 		TEXT,
	cite 		TEXT,
	publisher 	TEXT,
	note 		TEXT,
	crossref 	TEXT,
	isbn 		TEXT,
	series 		TEXT,
	school 		TEXT,
	chapter 	TEXT,
	publnr 		TEXT,
	stream 		TEXT,
	rel 		TEXT,
	aux 		TEXT,
	label 		TEXT,
	type_attr 	TEXT,
	href 		TEXT,
	orcid 		TEXT,
	bibtex 		TEXT,
	ref 		TEXT,
	sort 		TEXT,
	uri 		TEXT
);

CREATE TABLE IF NOT EXISTS pubschema_staging.rawAuthors(
	pubkey		TEXT,
	authorname	TEXT,
	authoralias	TEXT
);

CREATE TABLE IF NOT EXISTS pubschema.authors(
	aid 		SERIAL PRIMARY KEY,
	name 		TEXT
);

CREATE TABLE IF NOT EXISTS pubschema.publications(
	pid 		SERIAL PRIMARY KEY,
	pubkey 		TEXT NOT NULL,
	subclass 	TEXT NOT NULL,
	title 		TEXT,
	booktitle 	TEXT, 
	year 		SMALLINT,
	month 		TEXT,
	publisher 	TEXT,
	journal 	TEXT,
	crossref 	TEXT, -- temporary column
	cid 		INT -- FK to publications
);

CREATE TABLE IF NOT EXISTS pubschema.authored(
	pid			INT, -- FK to publications
	aid			INT -- FK to authors
);


/* Populate `pubschema_staging` Tables

Import the parsed CSV file into `rawData`

Do the following extraction for `rawAuthors` table:
- Extract the author name list for each publications 
- Extract author's aliases from `www` data
The `www` data contains list of author's aliases with the first name as the primary reference
More info: https://dblp.org/faq/1474690.html

*/

COPY pubschema_staging.rawData
FROM 'C:\temp\dblp_parsed_final.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"');
-- Query returned successfully in 40 secs 291 msec.

INSERT INTO pubschema_staging.rawAuthors
WITH
	unnested AS(
		SELECT type, key, TO_DATE(mdate, 'YYYY-MM-DD') AS mdate,
			TRIM((REGEXP_SPLIT_TO_ARRAY(author, ';'))[1]) AS authorname,
			TRIM(REGEXP_SPLIT_TO_TABLE(author, ';')) AS authoralias
		FROM pubschema_staging.rawData
	),
	aliases AS(
		SELECT authorname, authoralias
		FROM unnested
		WHERE type = 'www' AND key like 'homepages/%'
		ORDER BY authoralias, mdate DESC
	)
SELECT DISTINCT
	key AS pubkey,
	COALESCE(a.authorname, u.authoralias) AS authorname,
	u.authoralias AS authoralias
FROM unnested u
LEFT JOIN aliases a 
	USING(authoralias);
-- Query returned successfully in 4 min 26 secs.

/* Populate `pubschema` Tables */

-- Add relevant field attributes from `rawData` to `Publications`
INSERT INTO pubschema.publications(
	pubkey, subclass, title, booktitle, 
	year, month, publisher, journal, crossref
)
SELECT
	key	AS pubkey,
	type AS subclass,
	title,
	booktitle,
	CAST(year AS SMALLINT) AS year,
	month,
	publisher,
	journal,
	crossref
FROM pubschema_staging.rawData;
-- Query returned successfully in 42 secs 370 msec.


-- Add distinct names from unnested and mapped `rawAuthor` table to `Authors`
INSERT INTO pubschema.authors(name)
SELECT DISTINCT authorname AS name
FROM pubschema_staging.rawAuthors;
-- Query returned successfully in 22 secs 389 msec.


-- Add the crossref foreign key of `Publications`
UPDATE pubschema.publications AS pref
SET cid = psource.pid
FROM pubschema.publications AS psource
WHERE pref.crossref = psource.pubkey 
	AND pref.crossref IS NOT NULL;
-- Query returned successfully in 1 min 26 secs.


-- Add the publication and author foreign key of `Authored`
INSERT INTO pubschema.authored(pid, aid)
SELECT DISTINCT p.pid, a.aid
FROM pubschema_staging.rawAuthors ra
INNER JOIN pubschema.publications p
	ON ra.pubkey = p.pubkey
INNER JOIN pubschema.authors a
	ON ra.authorname = a.name;
-- Query returned successfully in 2 min 47 secs. (12 mins with FK constraint)


/* Table schema alterations 

Drop `crossref` in `Publications` to remove transitive dependency
Define foreign key constraints to the respective table:
- CID for `Publications` crossreference to `Publications`
- AID and PID for `Authored` referencing `Authors` and `Publications` respectively

*/

ALTER TABLE pubschema.publications
ADD FOREIGN KEY (cid) REFERENCES pubschema.publications,
DROP COLUMN IF EXISTS crossref;

ALTER TABLE pubschema.authored
ADD PRIMARY KEY (aid, pid),
ADD FOREIGN KEY (aid) REFERENCES pubschema.authors,
ADD FOREIGN KEY (pid) REFERENCES pubschema.publications;


/* Drop temp tables */
DROP TABLE IF EXISTS pubschema_staging.rawData;
DROP TABLE IF EXISTS pubschema_staging.rawAuthors;
DROP SCHEMA IF EXISTS pubschema_staging;