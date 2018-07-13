# txed
Texas public education data cleansing and database loading

Basic ETL flow is:
- Download (frequently dirty and incomplete) data from TAPR/AEIS (depending upon the school year in question) using the download-data.sh script. This is not guaranteed to work as currently written, as the link structure can change without notice.
- Load the data using the Python script in src/main.py

Note that main.py needs an update to pull the database username, password, hostname, and schema that you want to modify. It's hardcoded at the moment, and this is a Very Bad Thing. The dummy values in there should *not* be replaced, but should instead be removed entirely in favor of pulling from configuration outside the code - e.g., from a command-line switch or environment variable.

Easiest way to read the code at the moment is to start at the bottom of the file, looking at the commented lines. Read backwards from there!
