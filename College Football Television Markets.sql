IF DB_ID('DataProjects') IS NULL
	CREATE DATABASE DataProjects;
GO

USE DataProjects
GO

DROP TABLE IF EXISTS [dbo].[TelevisionMarkets];

CREATE TABLE [dbo].[TelevisionMarkets](
	[Rank] [varchar](max) NULL,
	[Market] [varchar](max) NULL,
	[State] [varchar](max) NULL,
	[Counties] [varchar](max) NULL,
	[TV_households] [varchar](max) NULL,
	[Local_ABC_affiliate] [varchar](max) NULL,
	[Local_CBS_affiliate] [varchar](max) NULL,
	[Local_NBC_affiliate] [varchar](max) NULL,
	[Local_Fox_affiliate] [varchar](max) NULL,
	[Local_CW_affiliate] [varchar](max) NULL,
	[Local_MyNetworkTV_affiliate] [varchar](max) NULL,
	[Local_Ion.Television_affiliate] [varchar](max) NULL,
	[Local_Telemundo_affiliate] [varchar](max) NULL,
	[Local_Univision_affiliate] [varchar](max) NULL,
	[Local_PBS_member_stations] [varchar](max) NULL,
	[Local_Independent_stations] [varchar](max) NULL,
	[Other_significant_television_stations] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

--Use stored proc [sp_execute_external_script] with the R language to
--scrape a wikipedia web page with data for US television markets.
--The format of the html table is what dictated the table definition of 
--[dbo].[TelevisionMarkets] (above). We'll insert the output of [sp_execute_external_script]
--directly into the table.
INSERT INTO [dbo].[TelevisionMarkets]
EXEC sp_execute_external_script 
	@language = N'R',
	@script = N'library(RCurl)
library(XML)
library(jsonlite)
library(httr)

#type.convert - sets appropriate class for each data frame variable/column.
TypeConvertDataFrameList <- function(lsDataFrame){
  lapply(lsDataFrame, type.convert, as.is = TRUE)
}

url <- "https://en.wikipedia.org/wiki/List_of_United_States_television_markets"
html <- httr::content(httr::GET(url), as = "text")

dfHtmlTableData <- XML::readHTMLTable(html, header = TRUE,
                                      as.data.frame = TRUE,
                                      stringsAsFactors = FALSE)
#remove null elements from list.
dfHtmlTableData <- dfHtmlTableData[!sapply(dfHtmlTableData, is.null)]

#convert data frame variables to appropriate classes.
dfHtmlTableData <- lapply(dfHtmlTableData, TypeConvertDataFrameList)

OutputDataSet <- as.data.frame(dfHtmlTableData[1])';

--SELECT * FROM dbo.TelevisionMarkets

DROP TABLE IF EXISTS [dbo].[FbsFootballPrograms];

CREATE TABLE [dbo].[FbsFootballPrograms](
	[Team] [varchar](256) NULL,
	[Nickname] [varchar](256) NULL,
	[City] [varchar](256) NULL,
	[State] [varchar](256) NULL,
	[Enrollment] [varchar](256) NULL,
	[CurrentConference] [varchar](256) NULL,
	[FormerConferences] [varchar](512) NULL,
	[FirstPlayed] [varchar](256) NULL,
	[JoinedFBS] [varchar](256) NULL
) ON [PRIMARY]
GO

--Delete any "garbage" rows that don't have a State.
DELETE FROM dbo.TelevisionMarkets
WHERE State IS NULL;

--Use stored proc [sp_execute_external_script] with the R language to
--scrape a wikipedia web page with data for US colleges that play Division I football.
--The format of the html table is what dictated the table definition of 
--[dbo].[FbsFootballPrograms] (above). We'll insert the output of [sp_execute_external_script]
--directly into the table.
INSERT INTO [dbo].[FbsFootballPrograms]
EXEC sp_execute_external_script 
	@language = N'R',
	@script = N'library(RCurl)
library(XML)
library(jsonlite)
library(httr)

#type.convert - sets appropriate class for each data frame variable/column.
TypeConvertDataFrameList <- function(lsDataFrame){
  lapply(lsDataFrame, type.convert, as.is = TRUE)
}

url <- "https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_FBS_football_programs"
html <- httr::content(httr::GET(url), as = "text")

dfHtmlTableData <- XML::readHTMLTable(html, header = TRUE,
                                      as.data.frame = TRUE,
                                      stringsAsFactors = FALSE)
#remove null elements from list.
dfHtmlTableData <- dfHtmlTableData[!sapply(dfHtmlTableData, is.null)]

#convert data frame variables to appropriate classes.
dfHtmlTableData <- lapply(dfHtmlTableData, TypeConvertDataFrameList)

OutputDataSet <- as.data.frame(dfHtmlTableData[1])';

--Clean up data in table [dbo].[FbsFootballPrograms]
UPDATE dbo.FbsFootballPrograms SET State = REPLACE(State, '''', '')
UPDATE dbo.FbsFootballPrograms SET City = REPLACE(City, '[n 1]', '')
UPDATE dbo.FbsFootballPrograms SET City = REPLACE(City, '[n 2]', '')
UPDATE dbo.FbsFootballPrograms SET City = REPLACE(City, '[n 3]', '')
UPDATE dbo.FbsFootballPrograms SET City = REPLACE(City, '[n 4]', '')
UPDATE dbo.FbsFootballPrograms SET City = REPLACE(City, '[n 5]', '')
UPDATE dbo.FbsFootballPrograms SET City = REPLACE(City, '[n 6]', '')
UPDATE dbo.FbsFootballPrograms SET City = REPLACE(City, '[n 7]', '')
UPDATE dbo.FbsFootballPrograms SET City = REPLACE(City, '[n 8]', '')
UPDATE dbo.FbsFootballPrograms SET City = REPLACE(City, '[n 9]', '')
UPDATE dbo.FbsFootballPrograms SET CurrentConference = REPLACE(CurrentConference, '[n 3]', '')
UPDATE dbo.FbsFootballPrograms SET CurrentConference = REPLACE(CurrentConference, '[n 5]', '')
UPDATE dbo.FbsFootballPrograms SET CurrentConference = REPLACE(CurrentConference, '[n 6]', '')

--This gives us the TV market of more than half of the colleges.
--Many colleges are not in cities primarily identified by the market name.
SELECT 
	t.[Rank] AS Market_Rank,
	t.[Market] AS TV_Market,
	p.Team, p.City, p.State, p.CurrentConference AS Conference
FROM dbo.FbsFootballPrograms p
JOIN US_LOCATIONS.dbo.US_States s
	ON s.STATE_NAME = p.[State]
LEFT JOIN dbo.TelevisionMarkets t
	ON t.[State] = p.[State]
	AND t.[Market] LIKE '%' + p.City + '%'
GO

--The television markets table includes the counties that comprise each market.
--With a cross-reference table of cities/counties, we can identify the television 
--market of the remaining colleges.

--Run this script to create a [US_LOCATIONS] database with tables for US states and cities: 
	--https://raw.githubusercontent.com/BeginTry/US-Cities-Database/main/US_cities_SqlServer.sql

--For convenience, create a VIEW.
CREATE OR ALTER VIEW dbo.CollegeFootballTvMarkets
AS
SELECT 
	COALESCE(t.[Rank], mk.[Rank], mk2.[Rank]) AS Market_Rank,
	COALESCE(t.[Market], mk.[Market], mk2.[Market]) AS TV_Market,
	p.Team, p.City, p.State, p.CurrentConference AS Conference
FROM dbo.FbsFootballPrograms p
JOIN US_LOCATIONS.dbo.US_States s
	ON s.STATE_NAME = p.[State]
LEFT JOIN dbo.TelevisionMarkets t
	ON t.[State] = p.[State]
	AND t.[Market] LIKE '%' + p.City + '%'
OUTER APPLY 
(
	--Try to match the city of a college to the market,
	--based on the city, state, and county.
	--We'll assume the college state and television market state are the same.
	SELECT TOP(1) t2.[Rank], t2.[Market]
	FROM dbo.TelevisionMarkets t2
	JOIN US_LOCATIONS.dbo.US_Cities c
		ON c.StateID = s.StateID
		AND c.City = p.City
		AND REPLACE(t2.[Counties], '''', '')  LIKE '%' + REPLACE(c.COUNTY, '''', '') + '%'
	WHERE t2.[State] = p.[State]
) AS mk
OUTER APPLY
(
	--Try to match the city of a college to the market,
	--based on the city, state, and county.
	--We'll assume the college state and television market state are different.
	SELECT TOP(1) t3.[Rank], t3.[Market]
	FROM US_LOCATIONS.dbo.US_Cities c
	JOIN dbo.TelevisionMarkets t3
		ON REPLACE(t3.[Counties], '''', '') LIKE '%' + p.[State] + ':%' + REPLACE(c.COUNTY, '''', '') + '%'
	WHERE c.StateID = s.StateID
	AND c.City = p.City
	AND t3.[State] <> p.[State]
) AS mk2
GO

--We're almost there. We've got the televsion market of every college except for five.
SELECT *
FROM dbo.CollegeFootballTvMarkets m
ORDER BY TRY_CAST(m.Market_Rank AS INT);


--Some data sleuthing tells us the [City] data in table [dbo].[FbsFootballPrograms]
--for the missing five does not exist in table [US_LOCATIONS].[dbo].[US_Cities].
--Let's make some manual updates so our data is consistent.
UPDATE dbo.FbsFootballPrograms SET City = 'Storrs Mansfield' WHERE City = 'Storrs'
UPDATE dbo.FbsFootballPrograms SET City = 'Miami' WHERE City = 'Coral Gables'
UPDATE dbo.FbsFootballPrograms SET City = 'Urbana' WHERE City = 'Urbana–Champaign'
UPDATE dbo.FbsFootballPrograms SET City = 'Dallas' WHERE City = 'University Park' AND State = 'Texas'
GO

--Now we have all the markets except for one.
SELECT *
FROM dbo.CollegeFootballTvMarkets m
ORDER BY TRY_CAST(m.Market_Rank AS INT);

--Some more data sleuthing shows us there is a county name inconsistency for Dona Aña county in New Mexico.
--Table [US_LOCATIONS].[dbo].[US_Cities] will be our source of truth: 'ñ' gets replace with 'n'
--in table [dbo].[TelevisionMarkets]
UPDATE dbo.TelevisionMarkets
SET Counties = REPLACE(Counties, 'ñ', 'n')
WHERE Counties LIKE '%ñ%';

--Now we have all 130 college markets.
SELECT *
FROM dbo.CollegeFootballTvMarkets m
ORDER BY TRY_CAST(m.Market_Rank AS INT);
