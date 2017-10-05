# hzn-export
Hzn to DLS export script

This script exports MARC records from Horizon using a date range or sql criteria. 

options (all datetimes expected in iso 8601 format (YYYYMMDDHHMMSS)):

* -b: boolean switch to export bib records
* -a: boolean switch to export auth records
* -m: modified since. export records modified since this time
* -u: modified until. don't export records modified after this time
* -s: sql query: export records with bib/auth#s returned by this query
* -o: directory to write output file to

What it does:

Loads a bunch of data it needs to add to records:
1. s3 data for creating FFT fields
2. audit data from the Hzn database
3. item data from the Hzn database 

Then filters out non-exportable records:
1. does not contain 191 or 791 field or exact match "DHU" in 039$b

Then performs the following operations on each record:

1. xrefs: for authroty-controlled fields, puts the auth# of the linked auth record (the xref) in subfield 0
2. 000: chop off illegal characters in the leader (field 000) that are contained in a some older records (leader must be exactly 24 chars)
3. 005: delete this field as per vendor
4. 007: create this field using defined rules
  * 4.1 If 245 contains \[cartographic material\] add 007 a
  * 4.2 If 245 contains \[video recording\] add 007 v
  * 4.3 If 245 contains \[sound recording\] OR if 191 contains ORAL HISTORY add 007 s
5. 020: delete subfield c
6. 035: create and add the Hzn id#; add prefix to specify source of ID; check if any existing 035 values are duplicated and correct; place old value in subfield z
7. 150: for auth records, change 150 to 151 and 550 to 551 for geogpraphic terms
8. 4xx: for auth records, delete the xrefs for 4xx fields (alt labels no longer treated as linked data fields). 
9. 650: change tag to 651 for geo terms
10. 856: create FFT fields pointing to s3 file store if DHL owns file; delete 856 fields for which was created
11. 949: add Hzn item information from control Hzn tables, per MARC standards for 949
12. 967: change all 968 and 969 tags to 967
13. 980: create field to denote authority type collection per defined rules
  * 13.1 If MARC record is an anuthority record 980__a AUTHORITY, and... (2 989 fields by authority record)
    * 13.1.1 If field 100 add 980__a PERSONAL
    * 13.1.2 If field 110 add 980__a CORPORATE
    * 13.1.3 If field 111 add 980__a MEETING
    * 13.1.4 If field 130 add 980__a UNIFORM
    * 13.1.5 If field 130 add 980__a TOPICAL
    * 13.1.6 If field 130 add 980__a GEOGRAPHIC
    * 13.1.7 If field 190 add 980__a SYMBOL
    * 13.1.8 If field 191 add 980__a AGENDA
  13.2 Else MARC record is a bib record add 980__a BIB
14. 989: create field using defined rules. If the record is a bibliographic record (980__a BIB) perform the rules in the following order:
  * 14.1 if 245:"\*\[cartographic material\]\*" OR 007:"a" OR 089__b:"B28" OR 191__b:"ST/LEG/UNTS/Map\*"
    * add 989__a Maps
  * 14.2 if 089__b:"B22"
    * add 989__a Speeches
  * 14.3 if 089__b:"B23"
    * add 989__a Voting data
  * 14.4 if 245:/(video|sound) recording/ OR 007:"s" OR 007:"v" OR 191:"\*ORAL HISTORY\*"
    * add 989__a: Images and Sounds
  * 14.5 if 191__a:"\*/RES/\*"
    * add 989__ $aDocuments and Publications	$bResolutions and Decisions	$cResolutions
  * 14.6  if 191__a:"\*/DEC/\*" AND 089__b:"B01"
    * add 989__ $aDocuments and Publications  $bResolutions and Decisions	$cDecisions
  * 14.7  if 191__a:"\*/PRST/\*" OR 089__b:"B17"
    * add 989__ $aDocuments and Publications	$bResolutions and Decisions	$cPresidential Statements
  * 14.8 if 089__b:"B01" NOT 989__b:"Resolutions and Decisions"
    * add 989__ $aDocuments and Publications	$b Resolutions and Decisions
  * 14.9  if 089__b:"B15" AND 089__b:"B16"  NOT 245:"\*letter\*from the Secretary-General\*"
    * add 989__ $aDocuments and Publications	$bReports	$cSecretary-General's Reports 
  * 14.10 if 089__b:"B04"
    * add 989__ $aDocuments and Publications	$bReports	$cAnnual and Sessional Reports 
  * 14.11 if 089__b:"B14" NOT 089__b:"B04"
    * add 989__ $aDocuments and Publications	$bReports	$cPeriodic Reports 
  * 14.12 if 089__b:"B16" AND title:"\*Report\*" NOT 989__b:"Reports"
    * add 989__ $aDocuments and Publications	$bReports
  * 14.13 if 191__a:"\*/PV.\*"
    * add 989__ $aDocuments and Publications	$bMeeting Records	$cVerbatim Records
  * 14.14 if 191__a:"\*/SR.\*"
    * add 989__ $aDocuments and Publications	$bMeeting Records	$cSummary Records
  * 14.15 if 089__b:"B03" NOT 989__b:"Meeting Records"
    * add 989__ $aDocuments and Publications	$bMeeting Records
  * 14.16 if 089__b:"B15" NOT 245:"Report\*" NOT 989__c:"Secretary-General's*"
    * add 989__ $aDocuments and Publications	$bLetters and Notes Verbales $cSecretary-General's Letters
  * 14.17 if 089__b:"B18" NOT 989__b:"Letters\*"
    * add 989__ $aDocuments and Publications	$bLetters and Notes Verbales
  * 14.18 if 022:"%" OR 020:"%" OR 089__b:"B13" OR 079:"%"
    * add 989__ $aDocuments and Publications	$bPublications  
  * 14.19 if 089__b:"B08"
    * add 989__ $aDocuments and Publications	$bDraft Reports
  * 14.20 if 089__b:"B02"
    * add 989__ $aDocuments and Publications	$bDraft Resolutions and Decisions
  * 14.21 if 191__a:"\*/PRESS/\*" OR 089__b:"B20"
    * add 989__ $aDocuments and Publications	$bPress releases
  * 14.22 if 089__b:"B12" OR 191__a:/\/(SGB|AI|IC|AFS)\//
    * add 989__ $aDocuments and Publications	$bAdministrative Issuances
  * 14.23 if 089__b:"A19"
    * add 989__ $aDocuments and Publications	$bTreaties and Agreements
  * 14.24 if 089__b:"A15" OR 089__b:"B25"
    * add 989__ $aDocuments and Publications	$bLegal Cases and Opinions
  * 14.25 if 089__b:"B21" OR 191__a:"\*/NGO/\*"
    * add 989__ $aDocuments and Publications	$bNGO Written Statements
  * 14.26 if 191__a:"*/PET*" NOT 989__b:"Petitions"
    * add 989__ $aDocuments and Publications	$bPetitions
  * 14.27 if 089__b:"B24"
  * add 989__ $aDocuments and Publications	$bConcluding Observations and Recommendations
  * 14.28 if 005:% NOT 989:%
    * add 989__ $aDocuments and Publications       
15. 993: create using defined rules
  * 15.1 if presidential statement symbols patterns S/PRST/\[0-9\]\[0-9\]\[0-9\]\[0-9\]/\[0-9\]+ are found in 191 $e
    * get the symbol and add it in a new 9935_a
  * 15.2 if the record has a 993, search for the following patterns (& + Add.\[0-9\]+, corr.\[0-9], Rev.\[0-9])
    * remove the & and create a new 993, with the same indicator and add, corr, rev patterns at the end of the root symbol. 
    * For instance, a 9933 $a A/62/318 & Add.1 in Horizon shoud be split in two 9933, one with the root symbol only A/62/318, one with the addendum A/62/318/Add.1
  * 15.3 If there is a 996 in a bib record, and if 191 or 791 $b are A/, S/ A/HRC, get the body and the session in 191/791 $b and $c, search for the meeting number pattern in 996, reconstruct the symbol to store it in a new 993. 
    * if it is a GA emergency special session (A/ES-#), and if the session is greater than 7 the symbol is reconstructed as follow A/ES-\[session]/PV.\[meeting number]. Otherwise it is reconstructed as follow A/PV.\[meeting number]
    * if it is a GA special session (A/S-#), and if the session is greater than 5 the symbol is reconstructed as follow A/S-\[session]/PV.\[meeting number]. Otherwise it is reconstructed as follow A/PV.\[meeting number]
    * if this is a GA regular session A/ and the session is greater than 30, the reconstructed symbol is A/\[session]/.PV.\[meeting number]. Otherwise, it is A/PV.\[meeting number]
    * For other bodies,  the reconstructed symbol is \[body]/PV.\[meeting number]
16. 998: create field using Hzn audit data; put time of export in subfield z
