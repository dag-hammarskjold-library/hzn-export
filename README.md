# hzn-export
Hzn to DLS export script

This script exports MARC records from Horizon using a date range or sql criteria. 

options (all datetimes expected in iso 8601 format (YYYYMMDDHHMMSS)):

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
1. does not contain 191 or 791 field or contain "DHU" in 039$b

Then performs the following operations on each record:
1. xrefs: for authroty-controlled fields, puts the auth# of the linked auth record (the xref) in subfield 0
2. 000: chop off illegal characters in the leader (field 000) that are contained in a some older records (leader must be exactly 24 chars)
3. 005: delete this field as per vendor
4. 007: create this field using defined rules
5. 020: delete subfield c
6. 035: create and add the Hzn id#; add prefix to specify source of ID; check if any existing 035 values are duplicated and correct; place old value in subfield z
7. 150: for auth records, change 150 to 151 and 550 to 551 for geogpraphic terms
8. 4xx: for auth records, delete the xrefs for 4xx fields (alt labels no longer treated as linked data fields). 
9. 650: change tag to 651 for geo terms
10. 856: create FFT fields pointing to s3 file store if DHL owns file; delete 856 fields for which was created
11. 949: add Hzn item information from control Hzn tables, per MARC standards for 949
12. 967: change all 968 and 969 tags to 967
13. 980: create field to denote authority type collection per defined rules
14. 989: create field using defined rules
15. 993: create using defined rules
16. 996: create using defined rules
17. 998: create field using Hzn audit data; put time of export in subfield z
