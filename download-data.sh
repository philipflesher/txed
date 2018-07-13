#!/usr/bin/env bash

set -ex

mkdir -p downloads
mkdir -p source-data

download_aeis() {
  DATASET=$1
  SUMLEV=$2
  STARTING_RECORD=$3
  YEAR=$4
  KEYS=$5

  curl "https://rptsvr1.tea.texas.gov/cgi/sas/broker/$DATASET" \
-H 'Accept-Encoding: gzip, deflate, br' \
-H 'Accept-Language: en-US,en;q=0.8' \
-H 'Upgrade-Insecure-Requests: 1' \
-H 'Content-Type: application/x-www-form-urlencoded' \
-H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' \
-H 'Cache-Control: max-age=0' \
-H 'Connection: keep-alive' \
--data "_service=marykay&year4=$YEAR&year2=YEAR2&prgopt=$YEAR%2Fxplore%2Fgetdata.sas&_program=perfrept.perfmast.sas&dsname=$DATASET&sumlev=$SUMLEV&_debug=0&${STARTING_RECORD}&_saveas=${DATASET}&datafmt=C&$KEYS" \
--compressed \
--insecure \
-o source-data/"aeis-$DATASET-$YEAR".csv
}

download_tapr() {
  DATASET=$1
  SUMLEV=$2
  YEAR=$3

  YEAR2=${YEAR:2}

  curl 'https://rptsvr1.tea.texas.gov/cgi/sas/broker' \
-H 'Accept-Encoding: gzip, deflate, br' \
-H 'Accept-Language: en-US,en;q=0.8' \
-H 'Upgrade-Insecure-Requests: 1' \
-H 'Content-Type: application/x-www-form-urlencoded' \
-H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' \
-H 'Cache-Control: max-age=0' \
-H 'Connection: keep-alive' \
--data "_service=marykay&prgopt=$YEAR%2Ftapr%2Ftapr_download.sas&year4=$YEAR&year2=$YEAR2&topic=acct&_debug=0&title=Data+Download&_program=perfrept.perfmast.sas&sumlev=$SUMLEV&setpick=$DATASET" \
--compressed \
--insecure \
-o source-data/"tapr-$SUMLEV$DATASET-$YEAR".csv
}

download_aeis_dref() {
  YEAR=$1
  KEYS="key=NTYNAM+&key=OUNTY+&key=FLCHAR+&key=ISTNAM+&key=ecs+&key=EGION+"
  download_aeis 'dref' 'D' 'dist0=999999' $YEAR $KEYS
}

download_aeis_cref() {
  YEAR=$1
  KEYS="key=AMPNAM+&key=FLCHAR+&key=NTYNAM+&key=OUNTY+&key=ISTNAM+&key=RDSPAN+&key=AIRNAM+&key=AIRCAM+&key=EGION+&key=RDTYPE+"
  download_aeis 'cref' 'C' 'camp0=999999' $YEAR $KEYS
}

download_aeis_cstaf() {
  YEAR=$1
  KEYS="key=PSAMIF+&key=PSATOF+&key=PCTENG+&key=PCTFLA+&key=PCTG01+&key=PCTG02+&key=PCTG03+&key=PCTG04+&key=PCTG05+&key=PCTG06+&key=PCTGKG+&key=PCTMAT+&key=PCTGME+&key=PCTSCI+&key=PCTSOC+&key=PSOTOF+&key=PSETOF+&key=PSPTOF+&key=PSSTOS+&key=PSSTOF+&key=PSUTOS+&key=PSUTOF+&key=PST01S+&key=PST01F+&key=PST11S+&key=PST11F+&key=PST06S+&key=PST06F+&key=PST20S+&key=PST20F+&key=PSTBLF+&key=PSTASF+&key=PST00S+&key=PST00F+&key=PSTBIF+&key=PSTVOF+&key=PSTCOF+&key=PSTEXP+&key=PSTFEF+&key=PSTGIF+&key=PSTHIF+&key=PSTINF+&key=PSTMAF+&key=PSTOPF+&key=PSTPIF+&key=PSTREF+&key=PSTSPF+&key=PSTKID+&key=PSTTEN+&key=PSTTOS+&key=PSTTOF+&key=PSTTWF+&key=PSTWHF+"
  download_aeis 'cstaf' 'C' 'camp0=999999' $YEAR $KEYS
}

download_aeis_cstud() {
  YEAR=$1
  KEYS="key=PETBLA+&key=PETALL+&key=PETIND+&key=PETASI+&key=PETRSK+&key=PETBIL+&key=PETVOC+&key=PETGEE+&key=PETECO+&key=PETGIF+&key=PETG01+&key=PETG02+&key=PETG03+&key=PETG04+&key=PETG05+&key=PETG06+&key=PETG07+&key=PETG08+&key=ETG9+&key=ETG0+&key=ETG1+&key=ETG2+&key=PERR+&key=PERS+&key=0GM1+&key=0GR1+&key=PETHIS+&key=PETGKN+&key=PETLEP+&key=PETNED+&key=PETPCI+&key=PETGPK+&key=0GH1+&key=PETSPE+&key=PEMALL+&key=PETDIS+&key=PETTWO+&key=PETPAC&key=PETWHI+"
  download_aeis 'cstud' 'C' 'camp0=999999' $YEAR $KEYS
}

download_tapr_dref() {
  YEAR=$1
  download_tapr 'REF' 'D' $YEAR
}

download_tapr_cref() {
  YEAR=$1
  download_tapr 'REF' 'C' $YEAR
}

download_tapr_cprof() {
  YEAR=$1
  download_tapr 'PROF' 'C' $YEAR
}

download_financial_old() {
  YEAR=$1

  YEAR2=${YEAR:2}

  curl "https://rptsvr1.tea.texas.gov/school.finance/forecasting/downloads/Actual_Multifile/Actual$YEAR2.zip" \
-H 'Accept-Encoding: gzip, deflate, sdch, br' \
-H 'Accept-Language: en-US,en;q=0.8' \
-H 'Upgrade-Insecure-Requests: 1'  \
-H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' \
-H 'Connection: keep-alive' \
--compressed \
--insecure \
-o source-data/"financial-actual-$YEAR".zip
}

download_financial_new() {
  YEAR=$1

  case "$YEAR" in
    2016)
      DOWNLOAD_ID='51539613933'
      ;;

    2015)
      DOWNLOAD_ID='25769825542'
      ;;
     
    2014)
      DOWNLOAD_ID='25769820604'
      ;;
     
    2013)
      DOWNLOAD_ID='25769810374'
      ;;

    2012)
      DOWNLOAD_ID='25769804137'
      ;;
     
    *)
      echo "Invalid year passed to download_financial_new()"
      exit 1
  esac

  curl "https://tea.texas.gov/WorkArea/DownloadAsset.aspx?id=$DOWNLOAD_ID" \
-H 'Accept-Encoding: gzip, deflate, sdch' \
-H 'Accept-Language: en-US,en;q=0.8' \
-H 'Upgrade-Insecure-Requests: 1' \
-H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' \
-H 'Connection: keep-alive' \
--compressed \
-o source-data/"financial-actual-$YEAR".zip
}

download_financial() {
  YEAR=$1
  if [[ "$YEAR" -ge '2012' ]]; then
    download_financial_new $YEAR
  else
    download_financial_old $YEAR
  fi
}

# download_tapr_dref '2016'
# download_tapr_dref '2015'
# download_tapr_dref '2014'
# download_tapr_dref '2013'
# download_aeis_dref '2012'
# download_aeis_dref '2011'
# download_aeis_dref '2010'
# download_aeis_dref '2009'
# download_aeis_dref '2008'
# download_aeis_dref '2007'
# download_aeis_dref '2006'
# download_aeis_dref '2005'
# download_aeis_dref '2004'

# download_tapr_cref '2016'
# download_tapr_cref '2015'
# download_tapr_cref '2014'
# download_tapr_cref '2013'
# download_aeis_cref '2012'
# download_aeis_cref '2011'
# download_aeis_cref '2010'
# download_aeis_cref '2009'
# download_aeis_cref '2008'
# download_aeis_cref '2007'
# download_aeis_cref '2006'
# download_aeis_cref '2005'
# download_aeis_cref '2004'

# download_tapr_cprof '2016'
# download_tapr_cprof '2015'
# download_tapr_cprof '2014'
# download_tapr_cprof '2013'

# download_aeis_cstaf '2012'
# download_aeis_cstaf '2011'
# download_aeis_cstaf '2010'
# download_aeis_cstaf '2009'
# download_aeis_cstaf '2008'
# download_aeis_cstaf '2007'
# download_aeis_cstaf '2006'
# download_aeis_cstaf '2005'
# download_aeis_cstaf '2004'

# download_aeis_cstud '2012'
# download_aeis_cstud '2011'
# download_aeis_cstud '2010'
# download_aeis_cstud '2009'
# download_aeis_cstud '2008'
# download_aeis_cstud '2007'
# download_aeis_cstud '2006'
# download_aeis_cstud '2005'
# download_aeis_cstud '2004'

# download_financial '2016'
# download_financial '2015'
# download_financial '2014'
# download_financial '2013'
# download_financial '2012'
# download_financial '2011'
# download_financial '2010'
# download_financial '2009'
# download_financial '2008'
# download_financial '2007'
# download_financial '2006'
# download_financial '2005'
# download_financial '2004'
# download_financial '2003'
# download_financial '2002'
# download_financial '2001'
# download_financial '2000'
# download_financial '1999'
# download_financial '1998'
# download_financial '1997'
# download_financial '1996'
# download_financial '1995'
# download_financial '1994'
# download_financial '1993'
# download_financial '1992'
