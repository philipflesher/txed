#!/usr/bin/python

import csv
from decimal import Context, Decimal, ROUND_HALF_UP
import fileinput
import os
import pymysql.cursors
import zipfile

from pprint import pprint

# Before using this code, this needs to be modified to pull username and password from
# an environment variable, command-line switch, or other more secure method. Putting it
# into code, even temporarily, is an almost surefire way to leak credentials to the
# public at some point.
user = 'username'
password = 'password'

# Simiarly, the hostname of the database server should be pulled from some configuration
# elsewhere.
host = 'hostname.com'
default_db = 'mysql_schema_name'

def connect(db=None):
  return pymysql.connect(host=host,
                         user=user,
                         password=password,
                         db=db,
                         charset='utf8',
                         cursorclass=pymysql.cursors.DictCursor)

def get_source_data_csv(file_type, year, source_type=None):
  script_dir = os.path.dirname(__file__)
  source_data_dir = script_dir + '../source-data'
  source_type_prefix = ''
  if source_type is not None:
    source_type_prefix = '{0}-'.format(source_type)
  return source_data_dir + '/{0}{1}-{2}.csv'.format(source_type_prefix, file_type, year)

def get_financial_zip(year):
  script_dir = os.path.dirname(__file__)
  source_data_dir = script_dir + '../source-data'
  return source_data_dir + '/financial-actual-{0}.zip'.format(year)

def get_financial_unzipped_folder(year):
  script_dir = os.path.dirname(__file__)
  source_data_dir = script_dir + '../source-data'
  return source_data_dir + '/financial-actual-{0}'.format(year)

def get_aeis_dref_csv(year):
  return get_source_data_csv(source_type='aeis', file_type='dref', year=year)

def get_aeis_cref_csv(year):
  return get_source_data_csv(source_type='aeis', file_type='cref', year=year)

def get_aeis_cstaf_csv(year):
  return get_source_data_csv(source_type='aeis', file_type='cstaf', year=year)

def get_aeis_cstud_csv(year):
  return get_source_data_csv(source_type='aeis', file_type='cstud', year=year)

def get_tapr_dref_csv(year):
  return get_source_data_csv(source_type='tapr', file_type='DREF', year=year)

def get_tapr_cref_csv(year):
  return get_source_data_csv(source_type='tapr', file_type='CREF', year=year)

def get_tapr_cprof_csv(year):
  return get_source_data_csv(source_type='tapr', file_type='CPROF', year=year)

def get_course_enrollment_csv(year):
  return get_source_data_csv(file_type='course', year=year)

parse_decimal_context = Context(prec=8, rounding=ROUND_HALF_UP)

def parse_numeric_round_to_1_decimal_place(number_string):
  return parse_decimal_context.quantize(Decimal(number_string), Decimal('0.1')) if number_string is not '.' else None

def parse_numeric_round_to_0_decimal_places(number_string):
  return parse_decimal_context.quantize(Decimal(number_string), Decimal('1')) if number_string is not '.' else None

def extract_financial_zip(year):
  zip_file_name = get_financial_zip(year=year)
  unzipped_folder_name = get_financial_unzipped_folder(year=year)
  if not os.path.isdir(unzipped_folder_name):
    with zipfile.ZipFile(zip_file_name, 'r') as financial_zip:
      financial_zip.extractall(path=unzipped_folder_name)

def get_financial_fund_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]
  one_digit_year = str(year)[-1:]

  if 1992 <= year <= 1993:
    file_name = 'FUNDF'
  elif 1994 <= year <= 1995:
    file_name = 'FUND{0}F.txt'.format(two_digit_year)
  elif 1996 <= year <= 1997:
    file_name = 'FUND{0}F.TXT'.format(two_digit_year)
  elif 1998 == year:
    file_name = 'FUND{0}F.txt'.format(two_digit_year)
  elif 1999 <= year <= 2001:
    file_name = 'FND{0}FILE'.format(one_digit_year)
  elif 2002 == year:
    file_name = 'FUND{0}F.txt'.format(two_digit_year)
  elif 2003 == year:
    file_name = 'FUND{0}F.txt'.format(one_digit_year)
  elif 2004 == year:
    file_name = 'Fund{0}.txt'.format(two_digit_year)
  elif 2005 == year:
    file_name = 'FUND_{0}F.txt'.format(year)
  elif 2006 == year:
    file_name = 'FUND{0}F'.format(two_digit_year)
  elif 2007 <= year <= 2016:
    file_name = 'FUND_{0}F.TXT'.format(year)
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_function_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]
  one_digit_year = str(year)[-1:]

  if 1992 <= year <= 1993:
    file_name = 'FUNCTNF'
  elif 1994 <= year <= 1995:
    file_name = 'FUNCF.txt'
  elif 1996 <= year <= 1997:
    file_name = 'FUNC{0}F.TXT'.format(two_digit_year)
  elif 1998 == year:
    file_name = 'FUNC{0}F.txt'.format(two_digit_year)
  elif 1999 <= year <= 2001:
    file_name = 'FUN{0}FILE'.format(one_digit_year)
  elif 2002 == year:
    file_name = 'FUNC{0}F.txt'.format(two_digit_year)
  elif 2003 == year:
    file_name = 'FUNC{0}F.txt'.format(one_digit_year)
  elif 2004 == year:
    file_name = 'Function{0}.txt'.format(two_digit_year)
  elif 2005 == year:
    file_name = 'FUNCTION_{0}F.txt'.format(year)
  elif 2006 == year:
    file_name = 'FUNC{0}F'.format(two_digit_year)
  elif 2007 <= year <= 2016:
    file_name = 'FUNCTION_{0}F.TXT'.format(year)
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_object_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]
  one_digit_year = str(year)[-1:]

  if 1992 <= year <= 1993:
    file_name = 'OBJECTF'
  elif 1994 <= year <= 1995:
    file_name = 'OBJ{0}F.txt'.format(two_digit_year)
  elif 1996 <= year <= 1997:
    file_name = 'OBJ{0}F.TXT'.format(two_digit_year)
  elif 1998 == year:
    file_name = 'OBJ{0}F.txt'.format(two_digit_year)
  elif 1999 <= year <= 2001:
    file_name = 'OBJ{0}FILE'.format(one_digit_year)
  elif 2002 == year:
    file_name = 'OBJ{0}F.txt'.format(two_digit_year)
  elif 2003 == year:
    file_name = 'OBJ{0}F.txt'.format(one_digit_year)
  elif 2004 == year:
    file_name = 'Object{0}.txt'.format(two_digit_year)
  elif 2005 == year:
    file_name = 'OBJECT_{0}F.txt'.format(year)
  elif 2006 == year:
    file_name = 'OBJECT{0}F'.format(one_digit_year)
  elif 2007 <= year <= 2016:
    file_name = 'OBJECT_{0}F.TXT'.format(year)
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_program_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)

  if 1992 <= year <= 1993:
    file_name = 'PROGRAMF'
  elif 1994 <= year <= 1995:
    # The program data is missing for 1994 and 1995. Fortunately, the programs appear
    # to have not changed from at least 1993 to 1996, based upon examination of those
    # files. So use the 1996 data for 1994 and 1995.
    return get_financial_program_csv(year=1996)
  elif 1996 == year:
    file_name = 'PROGRAMF.TXT'
  elif 1997 <= year <= 2004:
    # The program data is missing for 1997 through 2004. Fortunately, the programs appear
    # to have not changed from at least 1996 to 2005, based upon examination of those
    # files. So use the 2005 data for 1997 through 2004.
    return get_financial_program_csv(year=2005)
  elif 2005 == year:
    file_name = 'PROGRAMF.txt'
  elif 2006 == year:
    file_name = 'PROGRAMF'
  elif 2007 <= year <= 2016:
    file_name = 'PROGRAMF.TXT'
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_program_intent_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]

  if 1992 <= year <= 1996:
    # No program intent data exists for years prior to 1997. And there are no references
    # to program intents prior to 1997 in the account data either, so we can simply ignore
    # these years.
    return None
  elif 1997 <= year <= 1998:
    file_name = 'PGMINF.TXT'
  elif 1999 <= year <= 2001:
    file_name = 'PGMIFILE'
  elif 2002 == year:
    file_name = 'PGMINF.txt'
  elif 2003 == year:
    file_name = 'PICF.txt'
  elif 2004 == year:
    file_name = 'ProgramIntentCode.txt'
  elif 2005 == year:
    file_name = 'PROGRAM_INTENTF.txt'
  elif 2006 == year:
    file_name = 'PROGINTF'
  elif 2007 == year:
    file_name = 'PROGRAM_INTENT{0}F.TXT'.format(two_digit_year)
  elif 2008 <= year <= 2016:
    file_name = 'PROGRAM_INTENT{0}F.TXT'.format(year)
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_unit_type_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)

  if 1992 <= year <= 2007:
    # No financial unit type data exists for years prior to 2008. But it appears that
    # financial unit types have never changed, so we can use the 2008 data to backfill.
    return get_financial_unit_type_csv(year=2008)
  elif 2008 <= year <= 2016:
    file_name = 'FIN_UNIT_TYPEF.TXT'
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_unit_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]
  one_digit_year = str(year)[-1:]

  if 1992 <= year <= 1995:
    # No financial unit data exists for years prior to 1996. Although there are
    # references to financial units in the account data for these years, we have
    # to simply insert NULL for them.
    return None
  elif 1996 <= year <= 1997:
    file_name = 'FINU{0}F.TXT'.format(two_digit_year)
  elif 1998 == year:
    file_name = 'FINU{0}F.txt'.format(two_digit_year)
  elif 1999 <= year <= 2000:
    file_name = 'FIU{0}FILE'.format(one_digit_year)
  elif 2001 == year:
    file_name = 'FIU{0}FILE.txt'.format(one_digit_year)
  elif 2002 == year:
    file_name = 'FINU{0}F.txt'.format(two_digit_year)
  elif 2003 == year:
    # No financial unit data exists for 2003. Although there are
    # references to financial units in the account data for 2003, we have
    # to simply insert NULL for them.
    return None
  elif 2004 == year:
    file_name = 'FinUnit{0}.txt'.format(two_digit_year)
  elif 2005 == year:
    file_name = 'FIN_UNITF.txt'
  elif 2006 == year:
    file_name = 'FINUNITF'
  elif 2007 <= year <= 2016:
    file_name = 'FIN_UNIT_{0}F.TXT'.format(year)
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_account_csvs(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]
  one_digit_year = str(year)[-1:]

  if 1992 <= year <= 1993:
    file_names = [ 'ACT{0}FL'.format(two_digit_year) ]
  elif 1994 <= year <= 1995:
    file_names = [ 'ACT{0}FL.txt'.format(two_digit_year) ]
  elif 1996 <= year <= 1997:
    file_names = [ 'ACT{0}FL.TXT'.format(two_digit_year) ]
  elif 1998 == year:
    file_names = ['ACT{0}AFL'.format(two_digit_year), 'ACT{0}BFL.txt'.format(two_digit_year) ]
  elif 1999 == year:
    file_names = [ 'ACT{0}FILE'.format(one_digit_year) ]
  elif 2000 <= year <= 2001:
    file_names = ['ACT{0}AFLE'.format(one_digit_year), 'ACT{0}BFLE'.format(one_digit_year) ]
  elif 2002 == year:
    file_names = [ 'ACT{0}FL.txt'.format(two_digit_year) ]
  elif 2003 == year:
    file_names = [ 'ACT{0}F.txt'.format(one_digit_year) ]
  elif 2004 == year:
    file_names = [ 'Actual{0}All.txt'.format(two_digit_year) ]
  elif 2005 == year:
    file_names = [ 'ACTUAL_{0}F.txt'.format(year) ]
  elif 2006 == year:
    file_names = [ 'ACTUAL{0}F'.format(one_digit_year) ]
  elif 2007 <= year <= 2016:
    file_names = [ 'ACTUAL_{0}F.TXT'.format(year) ]
  else:
    raise Exception('Unhandled value for year')

  full_file_names = []
  for file_name in file_names:
    full_file_names.append('{0}/{1}'.format(folder_name, file_name))

  return full_file_names

def get_financial_charter_asset_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]
  one_digit_year = str(year)[-1:]

  if 1992 <= year <= 1996:
    return None
  elif 1997 == year:
    file_name = 'CSASTF.TXT'.format(two_digit_year)
  elif 1998 == year:
    file_name = 'CSAST{0}F.txt'.format(two_digit_year)
  elif 1999 <= year <= 2001:
    file_name = 'Css{0}file'.format(one_digit_year)
  elif 2002 == year:
    file_name = 'CSAST{0}F.txt'.format(two_digit_year)
  elif 2003 == year:
    file_name = 'CASSETF.txt'
  elif 2004 == year:
    file_name = 'CharterAsset{0}.txt'.format(two_digit_year)
  elif 2005 == year:
    file_name = 'CS_NONPROF_ASSETF.txt'
  elif 2006 == year:
    file_name = 'NASSETF'
  elif 2007 <= year <= 2016:
    file_name = 'CS_NONPROF_ASSETF.TXT'
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_charter_function_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]
  one_digit_year = str(year)[-1:]

  if 1992 <= year <= 1996:
    return None
  elif 1997 == year:
    file_name = 'CSFUNCF.TXT'
  elif 1998 == year:
    file_name = 'CSFUNCF.txt'
  elif 1999 <= year <= 2001:
    return None
  elif 2002 == year:
    file_name = 'CSFUNCF.txt'
  elif 2003 == year:
    file_name = 'CFUNCF.txt'
  elif 2004 == year:
    file_name = 'CharterFunction.txt'
  elif 2005 == year:
    file_name = 'CS_NONPROF_FUNCF.txt'
  elif 2006 == year:
    file_name = 'NFUNCF'
  elif 2007 <= year <= 2016:
    file_name = 'CS_NONPROF_FUNCF.TXT'
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_charter_object_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]
  one_digit_year = str(year)[-1:]

  if 1992 <= year <= 1996:
    return None
  elif 1997 == year:
    file_name = 'CSOBJF.TXT'
  elif 1998 == year:
    file_name = 'CSOBJF.txt'
  elif 1999 <= year <= 2001:
    file_name = 'CSOBFILE'
  elif 2002 == year:
    file_name = 'CSOBJF.txt'
  elif 2003 == year:
    file_name = 'COBJF.txt'
  elif 2004 == year:
    file_name = 'CharterObject.txt'
  elif 2005 == year:
    file_name = 'CS_NONPROF_OBJECTF.txt'
  elif 2006 == year:
    file_name = 'NONOBJF'
  elif 2007 == year:
    file_name = 'CS_NONPROF_OBJECTF.TXT'
  elif 2008 <= year <= 2016:
    file_name = 'CS_NONPROF_OBJ{0}F.TXT'.format(two_digit_year)
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_charter_program_intent_csv(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]

  if 1992 <= year <= 1996:
    return None
  elif 1997 == year:
    file_name = 'CSPGMF.TXT'
  elif 1998 == year:
    file_name = 'CSPGMF.txt'
  elif 1999 == year:
    file_name = 'Cspfile'
  elif 2000 == year:
    file_name = 'CSP{0}FILE'.format(one_digit_year)
  elif 2001 == year:
    file_name = 'Cspfile'
  elif 2002 == year:
    file_name = 'CSPGMF.txt'
  elif 2003 == year:
    file_name = 'CPGMINF.txt'
  elif 2004 == year:
    file_name = 'CharterProgramIntent.txt'
  elif 2005 == year:
    file_name = 'CS_NONPROF_PGMINF.txt'
  elif 2006 == year:
    file_name = 'NPGMINF'
  elif 2007 <= year <= 2016:
    file_name = 'CS_NONPROF_PGMINF.TXT'
  else:
    raise Exception('Unhandled value for year')

  return '{0}/{1}'.format(folder_name, file_name)

def get_financial_charter_account_csvs(year):
  folder_name = get_financial_unzipped_folder(year=year)
  two_digit_year = str(year)[-2:]
  one_digit_year = str(year)[-1:]

  if 1992 <= year <= 1996:
    return None
  elif 1997 == year:
    file_names = [ 'CSACT{0}F.TXT'.format(two_digit_year) ]
  elif 1998 == year:
    file_names = [ 'CSACT{0}F.txt'.format(two_digit_year) ]
  elif 1999 <= year <= 2001:
    file_names = [ 'CSA{0}FILE'.format(one_digit_year) ]
  elif 2002 == year:
    file_names = [ 'CSACT{0}F.txt'.format(two_digit_year) ]
  elif 2003 == year:
    file_names = [ 'CHART{0}F.txt'.format(one_digit_year) ]
  elif 2004 == year:
    file_names = [ 'CharterActual{0}.txt'.format(two_digit_year) ]
  elif 2005 == year:
    file_names = [ 'CS_NONPROF_ACT{0}F.txt'.format(two_digit_year) ]
  elif 2006 == year:
    file_names = [ 'NONACTF' ]
  elif 2007 <= year <= 2016:
    file_names = [ 'CS_NONPROF_ACT{0}F.TXT'.format(two_digit_year) ]
  else:
    raise Exception('Unhandled value for year')

  full_file_names = []
  for file_name in file_names:
    full_file_names.append('{0}/{1}'.format(folder_name, file_name))

  return full_file_names

def get_financial_fund_data_format(year):
  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1992 <= year <= 2004:
    field_names = [ 'FUND', 'FUNDX', 'FUNDX_LONG', 'DTUPDATE' ]
  elif 2005 <= year <= 2016:
    field_names = [ 'FUND', 'FUNDX', 'FUNDX_LONG', 'PAYROLL_ELIG', 'BUDGET_ELIG', 'ACTUAL_ELIG', 'SSA_ACTUAL_ELIG', 'DTUPDATE' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_function_data_format(year):
  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1992 <= year <= 2004:
    field_names = [ 'FUNCTION', 'FUNCTIONX', 'FUNCTIONX_LONG', 'DTUPDATE' ]
  elif 2005 <= year <= 2016:
    field_names = [ 'FUNCTION', 'FUNCTIONX', 'FUNCTIONX_LONG', 'PAYROLL_ELIG', 'BUDGET_ELIG', 'ACTUAL_ELIG', 'DTUPDATE' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_object_data_format(year):
  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1992 <= year <= 2004:
    field_names = [ 'OBJECT', 'OBJECTX', 'OBJECTX_LONG', 'DTUPDATE' ]
  elif 2005 <= year <= 2016:
    field_names = [ 'OBJECT', 'OBJECTX', 'OBJECTX_LONG', 'PAYROLL_ELIG', 'BUDGET_ELIG', 'ACTUAL_ELIG', 'DTUPDATE' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_program_data_format(year):
  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1992 <= year <= 2016:
    field_names = [ 'PROGRAM', 'PROGRAMX', 'PROGRAMX_LONG', 'DTUPDATE' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_program_intent_data_format(year):
  # No program intent data exists for 1992 through 1996
  if (1992 <= year <= 1996):
    return None

  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1997 <= year <= 2003:
    field_names = [ 'PROGRAM_INTENT', 'PROGRAM_INTENTX', 'USER_UPDATE', 'PROGRAM_INTENTX_L', 'DTUPDATE', 'TIME_UPDATE' ]
  elif 2004 == year:
    field_names = [ 'PROGRAM_INTENT', 'PROGRAM_INTENTX', 'DTUPDATE', 'TIME_UPDATE', 'USER_UPDATE', 'PROGRAM_INTENTX_L' ]
  elif 2005 <= year <= 2016:
    field_names = [ 'PROGRAM_INTENT', 'PROGRAM_INTENTX', 'PAYROLL_ELIG', 'BUDGET_ELIG', 'ACTUAL_ELIG', 'LINE_ITEM', 'USER_UPDATE', 'PROGRAM_INTENTX_L', 'DTUPDATE', 'TIME_UPDATE' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_unit_type_data_format(year):
  format = { 'has_header': False }

  if 1992 <= year <= 2016:
    field_names = [ 'FIN_UNIT_TYPE', 'FIN_UNIT_TYPEX', 'FIN_UNIT_TYPEX_LG', 'ACTIVE_DT', 'INACTIVE_DT', 'DTUPDATE' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_unit_data_format(year):
  # No financial unit data exists for 1992 through 1995 nor for 2003
  if (1992 <= year <= 1995 or 2003 == year):
    return None

  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1996 <= year <= 2000:
    field_names = [ 'DISTRICT', 'DIST_ORG_ID', 'FIN_UNIT', 'FIN_UNIT_TYPE', 'FIN_UNIT_NAME', 'FIN_UNIT_ORG_ID', 'ACTIVE_DT', 'INACTIVE_DT', 'DTUPDATE' ]
  elif 2001 == year:
    field_names = [ 'DISTRICT', 'DIST_ORG_ID', 'FIN_UNIT', 'FIN_UNIT_TYPE', 'FIN_UNIT_NAME', 'FIN_UNIT_ORG_ID', 'DTUPDATE' ]
  elif 2002 == year:
    field_names = [ 'DISTRICT', 'DIST_ORG_ID', 'FIN_UNIT', 'FIN_UNIT_TYPE', 'FIN_UNIT_NAME', 'FIN_UNIT_ORG_ID', 'ACTIVE_DT', 'INACTIVE_DT', 'DTUPDATE' ]
  elif 2004 <= year <= 2016:
    field_names = [ 'DISTRICT', 'DIST_ORG_ID', 'FIN_UNIT', 'FIN_UNIT_TYPE', 'FIN_UNIT_NAME', 'FIN_UNIT_ORG_ID', 'ACTIVE_DT', 'INACTIVE_DT', 'DTUPDATE' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_account_data_format(year):
  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1992 <= year <= 1996:
    field_names = [ 'DISTRICT', 'FUND', 'FUNCTION', 'OBJECT', 'ORG', 'PROGRAM', 'CAMPUS', 'FUNDYEAR', 'ACTAMT', 'DTUPDATE' ]
  elif 1997 <= year <= 1999:
    field_names = [ 'DISTRICT', 'FUND', 'FUNCTION', 'OBJECT', 'FIN_UNIT', 'PROGRAM_INTENT', 'FUNDYEAR', 'ACTAMT', 'DTUPDATE' ]
  elif 2000 == year:
    field_names = [ 'DISTRICT', 'FUND', 'FUNCTION', 'OBJECT', 'FIN_UNIT', 'PROGRAM_INTENT', 'FUNDYEAR', 'ACTAMT' ]
  elif 2001 <= year <= 2002:
    field_names = [ 'DISTRICT', 'FUND', 'FUNCTION', 'OBJECT', 'FIN_UNIT', 'PROGRAM_INTENT', 'FUNDYEAR', 'ACTAMT', 'DTUPDATE' ]
  elif 2003 == year:
    field_names = [ 'DISTRICT', 'FUND', 'FUNCTION', 'OBJECT', 'FIN_UNIT', 'PROGRAM_INTENT', 'FUNDYEAR', 'ACTAMT' ]
  elif 2004 == year:
    field_names = [ 'DISTRICT', 'FUND', 'FUNDYEAR', 'FUNCTION', 'OBJECT', 'FIN_UNIT', 'PROGRAM_INTENT', 'ACTAMT' ]
  elif 2005 <= year <= 2016:
    field_names = [ 'DISTRICT', 'FUND', 'FUNCTION', 'OBJECT', 'FIN_UNIT', 'PROGRAM_INTENT', 'FUNDYEAR', 'ACTAMT', 'DTUPDATE' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_charter_asset_data_format(year):
  # No asset data exists for 1992 through 1996
  if (1992 <= year <= 1996):
    return None

  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1997 <= year <= 2016:
    field_names = [ 'CS_NONPROF_ASSET', 'CS_NONPROF_ASSETX', 'CS_NONPROF_ASSETXL' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_charter_function_data_format(year):
  # No function data exists for 1992 through 1996
  if (1992 <= year <= 1996):
    return None

  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1997 <= year <= 2016:
    field_names = [ 'CS_NONPROF_FUNC', 'CS_NONPROF_FUNCX', 'CS_NONPROF_FUNCXL' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_charter_object_data_format(year):
  # No object data exists for 1992 through 1996
  if (1992 <= year <= 1996):
    return None

  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1997 <= year <= 2016:
    field_names = [ 'CS_NONPROF_OBJ', 'CS_NONPROF_OBJX', 'CS_NONPROF_OBJXL' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_charter_program_intent_data_format(year):
  # No program intent data exists for 1992 through 1996
  if (1992 <= year <= 1996):
    return None

  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1997 <= year <= 2016:
    field_names = [ 'CS_NONPROF_PGMIN', 'CS_NONPROF_PGMINX', 'CS_NONPROF_PGMINXL' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def get_financial_charter_account_data_format(year):
  # No account data exists for 1992 through 1996
  if (1992 <= year <= 1996):
    return None

  format = { 'has_header': False }
  if year == 2004:
    format['has_header'] = True

  if 1997 <= year <= 2016:
    field_names = [ 'DISTRICT', 'CS_NONPROF_ASSET', 'CS_NONPROF_FUNC', 'CS_NONPROF_OBJ', 'FIN_UNIT', 'CS_NONPROF_PGMIN', 'FISCALYR', 'ACTAMT', 'DTUPDATE' ]
  else:
    raise Exception('Unhandled value for year')

  format['field_names'] = field_names
  return format

def create_db():
  conn = connect()
  try:
    with conn.cursor() as cursor:
      # cursor.execute("DROP DATABASE IF EXISTS %s" % default_db)
      # cursor.execute("CREATE DATABASE %s CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" % default_db)
      cursor.execute("USE %s" % default_db)
      # cursor.execute(
      #   "CREATE TABLE `school_years` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`name` VARCHAR(255) NOT NULL, "
      #     "`begin_year` INT NOT NULL, "
      #     "`end_year` INT NOT NULL, "
      #     "UNIQUE INDEX (`end_year`, `begin_year`), "
      #     "UNIQUE INDEX (`begin_year`, `end_year`)"
      #   ")"
      # )
      # cursor.execute(
      #   "INSERT INTO `school_years` (`name`, `begin_year`, `end_year`) VALUES "
      #     "('1991-1992', 1991, 1992),"
      #     "('1992-1993', 1992, 1993),"
      #     "('1993-1994', 1993, 1994),"
      #     "('1994-1995', 1994, 1995),"
      #     "('1995-1996', 1995, 1996),"
      #     "('1996-1997', 1996, 1997),"
      #     "('1997-1998', 1997, 1998),"
      #     "('1998-1999', 1998, 1999),"
      #     "('1999-2000', 1999, 2000),"
      #     "('2000-2001', 2000, 2001),"
      #     "('2001-2002', 2001, 2002),"
      #     "('2002-2003', 2002, 2003),"
      #     "('2003-2004', 2003, 2004),"
      #     "('2004-2005', 2004, 2005),"
      #     "('2005-2006', 2005, 2006),"
      #     "('2006-2007', 2006, 2007),"
      #     "('2007-2008', 2007, 2008),"
      #     "('2008-2009', 2008, 2009),"
      #     "('2009-2010', 2009, 2010),"
      #     "('2010-2011', 2010, 2011),"
      #     "('2011-2012', 2011, 2012),"
      #     "('2012-2013', 2012, 2013),"
      #     "('2013-2014', 2013, 2014),"
      #     "('2014-2015', 2014, 2015),"
      #     "('2015-2016', 2015, 2016)"
      # )
      # cursor.execute(
      #   "CREATE TABLE `grades` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`xid` CHAR(2) NOT NULL, "
      #     "`name` VARCHAR(255) NOT NULL, "
      #     "UNIQUE INDEX (`xid`)"
      #   ")"
      # )
      # cursor.execute(
      #   "INSERT INTO `grades` (`xid`, `name`) VALUES "
      #     "('EE', 'Early Education'),"
      #     "('PK', 'Pre-Kindergarten'),"
      #     "('KG', 'Kindergarten'),"
      #     "('01', '1st Grade'),"
      #     "('02', '2nd Grade'),"
      #     "('03', '3rd Grade'),"
      #     "('04', '4th Grade'),"
      #     "('05', '5th Grade'),"
      #     "('06', '6th Grade'),"
      #     "('07', '7th Grade'),"
      #     "('08', '8th Grade'),"
      #     "('09', '9th Grade'),"
      #     "('10', '10th Grade'),"
      #     "('11', '11th Grade'),"
      #     "('12', '12th Grade')"
      # )
      # cursor.execute(
      #   "CREATE TABLE `grade_types` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`xid` CHAR(1) NOT NULL, "
      #     "`name` VARCHAR(255) NOT NULL, "
      #     "UNIQUE INDEX (`xid`)"
      #   ")"
      # )
      # cursor.execute(
      #   "INSERT INTO `grade_types` (`xid`, `name`) VALUES "
      #     "('E', 'Elementary'),"
      #     "('M', 'Middle'),"
      #     "('S', 'Secondary/Senior'),"
      #     "('B', 'Both')"
      # )
      # cursor.execute(
      #   "CREATE TABLE `districts` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`xid` CHAR(6) NOT NULL, "
      #     "`name` VARCHAR(255) NOT NULL, "
      #     "`county_xid` CHAR(3) NOT NULL, "
      #     "`county_name` VARCHAR(255) NOT NULL, "
      #     "`region_xid` CHAR(2) NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `xid`), "
      #     "INDEX (`school_year_id`, `county_xid`), "
      #     "INDEX (`school_year_id`, `region_xid`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `campuses` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`xid` CHAR(9) NOT NULL, "
      #     "`name` VARCHAR(255) NOT NULL, "
      #     "`district_id` BIGINT NOT NULL, "
      #     "`min_grade_id` BIGINT NOT NULL, "
      #     "`max_grade_id` BIGINT NOT NULL, "
      #     "`grade_type_id` BIGINT NOT NULL, "
      #     "`is_charter` BOOLEAN NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `xid`), "
      #     "INDEX (`school_year_id`, `district_id`), "
      #     "INDEX (`school_year_id`, `min_grade_id`, `max_grade_id`), "
      #     "INDEX (`school_year_id`, `max_grade_id`, `min_grade_id`), "
      #     "INDEX (`school_year_id`, `grade_type_id`), "
      #     "INDEX (`school_year_id`, `is_charter`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`district_id`) REFERENCES `districts`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`min_grade_id`) REFERENCES `grades`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`max_grade_id`) REFERENCES `grades`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`grade_type_id`) REFERENCES `grade_types`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `staff` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`campus_id` BIGINT NOT NULL, "
      #     "`total_full_time_equivalent_count` DECIMAL(8, 1) NULL, "
      #     "`educational_aide_full_time_equivalent_count` DECIMAL(8, 1) NULL, "
      #     "`support_full_time_equivalent_count` DECIMAL(8, 1) NULL, "
      #     "`school_admin_full_time_equivalent_count` DECIMAL(8, 1) NULL, "
      #     "`teacher_full_time_equivalent_count` DECIMAL(8, 1) NULL, "
      #     "`professional_full_time_equivalent_count` DECIMAL(8, 1) NULL, "
      #     "`contract_services_full_time_equivalent_count` DECIMAL(8, 1) NULL, "
      #     "`teacher_base_salary_average` DECIMAL(8, 0) NULL, "
      #     "`teacher_experience_average` DECIMAL(8, 1) NULL, "
      #     "`teacher_tenure_average` DECIMAL(8, 1) NULL, "
      #     "`teacher_student_ratio` DECIMAL(8, 1) NULL, "
      #     "UNIQUE INDEX (`campus_id`), "
      #     "FOREIGN KEY (`campus_id`) REFERENCES `campuses`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `students` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`campus_id` BIGINT NOT NULL, "
      #     "`at_risk_count` DECIMAL(8, 0) NULL, "
      #     "`economically_disadvantaged_count` DECIMAL(8, 0) NULL, "
      #     "`gifted_and_talented_count` DECIMAL(8, 0) NULL, "
      #     "`bilingual_and_esl_count` DECIMAL(8, 0) NULL, "
      #     "`limited_english_proficient_count` DECIMAL(8, 0) NULL, "
      #     "`non_educationally_disadvantaged_count` DECIMAL(8, 0) NULL, "
      #     "`special_education_count` DECIMAL(8, 0) NULL, "
      #     "`career_and_technical_education_count` DECIMAL(8, 0) NULL, "
      #     "`mobility_count` DECIMAL(8, 0) NULL, "
      #     "`white_count` DECIMAL(8, 0) NULL, "
      #     "`african_american_count` DECIMAL(8, 0) NULL, "
      #     "`hispanic_count` DECIMAL(8, 0) NULL, "
      #     "`asian_and_pacific_islander_count` DECIMAL(8, 0) NULL, "
      #     "`other_race_count` DECIMAL(8, 0) NULL, "
      #     "`total_count` DECIMAL(8, 0) NULL, "
      #     "UNIQUE INDEX (`campus_id`), "
      #     "FOREIGN KEY (`campus_id`) REFERENCES `campuses`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `course_subject_areas` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`name` VARCHAR(30) NOT NULL, "
      #     "UNIQUE INDEX (`name`)"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `course_subjects` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`course_subject_area_id` BIGINT NOT NULL, "
      #     "`name` VARCHAR(30) NOT NULL, "
      #     "UNIQUE INDEX (`course_subject_area_id`, `name`), "
      #     "FOREIGN KEY (`course_subject_area_id`) REFERENCES `course_subject_areas`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `courses` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`xid` VARCHAR(8) NOT NULL, "
      #     "`course_subject_id` BIGINT NOT NULL, "
      #     "`name` VARCHAR(30) NOT NULL, "
      #     "`min_grade_id` BIGINT NULL, "
      #     "`max_grade_id` BIGINT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `xid`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`course_subject_id`) REFERENCES `course_subjects`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`min_grade_id`) REFERENCES `grades`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`max_grade_id`) REFERENCES `grades`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `course_enrollments` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`campus_id` BIGINT NOT NULL, "
      #     "`course_id` BIGINT NOT NULL, "
      #     "`teacher_full_time_equivalent_count` DECIMAL(8, 1) NULL, "
      #     "`enrollment_count` DECIMAL(8, 0) NULL, "
      #     "UNIQUE INDEX (`campus_id`, `course_id`), "
      #     "FOREIGN KEY (`campus_id`) REFERENCES `campuses`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`course_id`) REFERENCES `courses`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `financial_funds` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`xid` CHAR(3) NOT NULL, "
      #     "`description` VARCHAR(30) NOT NULL, "
      #     "`description_long` VARCHAR(75) NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `xid`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `financial_functions` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`xid` CHAR(2) NOT NULL, "
      #     "`description` VARCHAR(30) NOT NULL, "
      #     "`description_long` VARCHAR(75) NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `xid`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `financial_objects` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`xid` CHAR(4) NOT NULL, "
      #     "`description` VARCHAR(30) NOT NULL, "
      #     "`description_long` VARCHAR(75) NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `xid`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `financial_unit_types` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`xid` CHAR(2) NOT NULL, "
      #     "`description` VARCHAR(30) NOT NULL, "
      #     "`description_long` VARCHAR(75) NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `xid`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `financial_units` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`district_id` BIGINT NOT NULL, "
      #     "`xid` CHAR(3) NOT NULL, "
      #     "`financial_unit_type_id` BIGINT NOT NULL, "
      #     "`name` VARCHAR(50) NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `district_id`, `xid`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`district_id`) REFERENCES `districts`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`financial_unit_type_id`) REFERENCES `financial_unit_types`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `financial_programs` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`xid` CHAR(2) NOT NULL, "
      #     "`description` VARCHAR(30) NOT NULL, "
      #     "`description_long` VARCHAR(75) NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `xid`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `financial_program_intents` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`xid` CHAR(2) NOT NULL, "
      #     "`description` VARCHAR(30) NOT NULL, "
      #     "`description_long` VARCHAR(75) NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `xid`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )
      # cursor.execute(
      #   "CREATE TABLE `financial_accounts` ("
      #     "`id` BIGINT PRIMARY KEY AUTO_INCREMENT, "
      #     "`school_year_id` BIGINT NOT NULL, "
      #     "`district_id` BIGINT NOT NULL, "
      #     "`campus_id` BIGINT NULL, "
      #     "`financial_fund_id` BIGINT NOT NULL, "
      #     "`financial_function_id` BIGINT NOT NULL, "
      #     "`financial_object_id` BIGINT NULL, "
      #     "`financial_unit_id` BIGINT NULL, "
      #     "`financial_program_id` BIGINT NULL, "
      #     "`financial_program_intent_id` BIGINT NULL, "
      #     "`amount` DECIMAL(15, 0) NOT NULL, "
      #     "UNIQUE INDEX (`school_year_id`, `district_id`, `campus_id`, `financial_fund_id`, `financial_function_id`, `financial_object_id`, `financial_unit_id`, `financial_program_id`, `financial_program_intent_id`), "
      #     "INDEX (`school_year_id`, `district_id`, `campus_id`), "
      #     "FOREIGN KEY (`school_year_id`) REFERENCES `school_years`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`district_id`) REFERENCES `districts`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`campus_id`) REFERENCES `campuses`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`financial_fund_id`) REFERENCES `financial_funds`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`financial_function_id`) REFERENCES `financial_functions`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`financial_object_id`) REFERENCES `financial_objects`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`financial_unit_id`) REFERENCES `financial_units`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`financial_program_id`) REFERENCES `financial_programs`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION, "
      #     "FOREIGN KEY (`financial_program_intent_id`) REFERENCES `financial_program_intents`(`id`) ON UPDATE NO ACTION ON DELETE NO ACTION"
      #   ")"
      # )

    conn.commit()
  finally:
    conn.close()

def load_district_reference_file(csv_file, school_year_id, cursor):
  csv_reader = csv.DictReader(csv_file)

  new_rows = []
  for row in csv_reader:
    district_xid = row['DISTRICT']
    district_name = row['DISTNAME']
    county_name = row['CNTYNAME']
    county_xid = row['COUNTY']
    region_xid = row['REGION']

    new_row = (
      school_year_id,
      district_xid,
      district_name,
      county_xid,
      county_name,
      region_xid
    )
    new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `districts` ("
      "`school_year_id`, "
      "`xid`, "
      "`name`, "
      "`county_xid`, "
      "`county_name`, "
      "`region_xid`"
    ") VALUES ("
      "%s, %s, %s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`name` = VALUES(`name`), "
      "`county_xid` = VALUES(`county_xid`), "
      "`county_name` = VALUES(`county_name`), "
      "`region_xid` = VALUES(`region_xid`)",
    new_rows
  )

def load_campus_reference_file(csv_file, school_year_id, districts_by_xid, districts_by_other_data, grades_by_xid, grade_types_by_xid, cursor):
  csv_reader = csv.DictReader(csv_file)

  new_rows = []
  for row in csv_reader:
    campus_xid = row['CAMPUS']
    campus_name = row['CAMPNAME']
    is_charter_y_or_n = row['CFLCHART']
    grade_span = row['GRDSPAN']
    grade_type = row['GRDTYPE']

    # Get the district XID if it's available
    if 'DISTRICT' in row:
      district_xid = row['DISTRICT']
    else:
      # Use these fields to divine the district XID
      region_xid = row['REGION']
      county_xid = row['COUNTY']
      district_name = row['DISTNAME']
      district_xid = districts_by_other_data[region_xid][county_xid][district_name]['xid']

    grade_span_splits = grade_span.split(' - ')
    min_grade_xid = grade_span_splits[0]
    max_grade_xid = grade_span_splits[1]

    is_charter = None
    if is_charter_y_or_n == 'Y':
      is_charter = True
    elif is_charter_y_or_n == 'N':
      is_charter = False
    else:
      raise Exception('Invalid value for charter flag: ' + is_charter_y_or_n)

    district_id = districts_by_xid[district_xid]['id']
    min_grade_id = grades_by_xid[min_grade_xid]['id']
    max_grade_id = grades_by_xid[max_grade_xid]['id']
    grade_type_id = grade_types_by_xid[grade_type]['id']

    new_row = (
      school_year_id,
      campus_xid,
      campus_name,
      district_id,
      min_grade_id,
      max_grade_id,
      grade_type_id,
      is_charter
    )
    new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `campuses` ("
      "`school_year_id`, "
      "`xid`, "
      "`name`, "
      "`district_id`, "
      "`min_grade_id`, "
      "`max_grade_id`, "
      "`grade_type_id`, "
      "`is_charter`"
    ") VALUES ("
      "%s, %s, %s, %s, %s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`name` = VALUES(`name`), "
      "`district_id` = VALUES(`district_id`), "
      "`min_grade_id` = VALUES(`min_grade_id`), "
      "`max_grade_id` = VALUES(`max_grade_id`), "
      "`grade_type_id` = VALUES(`grade_type_id`), "
      "`is_charter` = VALUES(`is_charter`)",
    new_rows
  )

def load_staff_reference_file(csv_file, campuses_by_xid, cursor):
  csv_reader = csv.DictReader(csv_file)

  new_rows = []
  for row in csv_reader:
    campus_xid = row['CAMPUS']
    total_full_time_equivalent_count = parse_numeric_round_to_1_decimal_place(row['CPSATOFC'])
    educational_aide_full_time_equivalent_count = parse_numeric_round_to_1_decimal_place(row['CPSETOFC'])
    support_full_time_equivalent_count = parse_numeric_round_to_1_decimal_place(row['CPSUTOFC'])
    school_admin_full_time_equivalent_count = parse_numeric_round_to_1_decimal_place(row['CPSSTOFC'])
    teacher_full_time_equivalent_count = parse_numeric_round_to_1_decimal_place(row['CPSTTOFC'])
    professional_full_time_equivalent_count = parse_numeric_round_to_1_decimal_place(row['CPSPTOFC'])
    contract_services_full_time_equivalent_count = parse_numeric_round_to_1_decimal_place(row['CPSOTOFC'])
    teacher_base_salary_average = parse_numeric_round_to_0_decimal_places(row['CPSTTOSA'])
    teacher_experience_average = parse_numeric_round_to_1_decimal_place(row['CPSTEXPA'])
    teacher_tenure_average = parse_numeric_round_to_1_decimal_place(row['CPSTTENA'])
    teacher_student_ratio = parse_numeric_round_to_1_decimal_place(row['CPSTKIDR'])

    campus_id = campuses_by_xid[campus_xid]['id']

    new_row = (
      campus_id,
      total_full_time_equivalent_count,
      educational_aide_full_time_equivalent_count,
      support_full_time_equivalent_count,
      school_admin_full_time_equivalent_count,
      teacher_full_time_equivalent_count,
      professional_full_time_equivalent_count,
      contract_services_full_time_equivalent_count,
      teacher_base_salary_average,
      teacher_experience_average,
      teacher_tenure_average,
      teacher_student_ratio
    )
    new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `staff` ("
      "`campus_id`, "
      "`total_full_time_equivalent_count`, "
      "`educational_aide_full_time_equivalent_count`, "
      "`support_full_time_equivalent_count`, "
      "`school_admin_full_time_equivalent_count`, "
      "`teacher_full_time_equivalent_count`, "
      "`professional_full_time_equivalent_count`, "
      "`contract_services_full_time_equivalent_count`, "
      "`teacher_base_salary_average`, "
      "`teacher_experience_average`, "
      "`teacher_tenure_average`, "
      "`teacher_student_ratio`"
    ") VALUES ("
      "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`total_full_time_equivalent_count` = VALUES(`total_full_time_equivalent_count`), "
      "`educational_aide_full_time_equivalent_count` = VALUES(`educational_aide_full_time_equivalent_count`), "
      "`support_full_time_equivalent_count` = VALUES(`support_full_time_equivalent_count`), "
      "`school_admin_full_time_equivalent_count` = VALUES(`school_admin_full_time_equivalent_count`), "
      "`teacher_full_time_equivalent_count` = VALUES(`teacher_full_time_equivalent_count`), "
      "`professional_full_time_equivalent_count` = VALUES(`professional_full_time_equivalent_count`), "
      "`contract_services_full_time_equivalent_count` = VALUES(`contract_services_full_time_equivalent_count`), "
      "`teacher_base_salary_average` = VALUES(`teacher_base_salary_average`), "
      "`teacher_experience_average` = VALUES(`teacher_experience_average`), "
      "`teacher_tenure_average` = VALUES(`teacher_tenure_average`), "
      "`teacher_student_ratio` = VALUES(`teacher_student_ratio`)",
    new_rows
  )

def load_student_reference_file(csv_file, campuses_by_xid, cursor):
  csv_reader = csv.DictReader(csv_file)

  new_rows = []
  for row in csv_reader:
    campus_xid = row['CAMPUS']
    # Some student records have campus ID's that aren't in campus reference data - not sure why
    if campus_xid in campuses_by_xid:
      at_risk_count = parse_numeric_round_to_0_decimal_places(row['CPETRSKC'] if 'CPETRSKC' in row else '.')
      economically_disadvantaged_count = parse_numeric_round_to_0_decimal_places(row['CPETECOC'])
      gifted_and_talented_count = parse_numeric_round_to_0_decimal_places(row['CPETGIFC'])
      bilingual_and_esl_count = parse_numeric_round_to_0_decimal_places(row['CPETBILC'])
      limited_english_proficient_count = parse_numeric_round_to_0_decimal_places(row['CPETLEPC'])
      non_educationally_disadvantaged_count = parse_numeric_round_to_0_decimal_places(row['CPETNEDC'] if 'CPETNEDC' in row else '.')
      special_education_count = parse_numeric_round_to_0_decimal_places(row['CPETSPEC'])
      career_and_technical_education_count = parse_numeric_round_to_0_decimal_places(row['CPETVOCC'])
      mobility_count = parse_numeric_round_to_0_decimal_places(row['CPEMALLC'])
      white_count = parse_numeric_round_to_0_decimal_places(row['CPETWHIC'])
      african_american_count = parse_numeric_round_to_0_decimal_places(row['CPETBLAC'])
      hispanic_count = parse_numeric_round_to_0_decimal_places(row['CPETHISC'])
      if ('CPETPACC' in row):
        asian_and_pacific_islander_count = parse_numeric_round_to_0_decimal_places(row['CPETPACC'])
      else:
        asian_and_pacific_islander_count = parse_numeric_round_to_0_decimal_places(row['CPETPCIC']) + parse_numeric_round_to_0_decimal_places(row['CPETASIC'])
      total_count = parse_numeric_round_to_0_decimal_places(row['CPETALLC'])

      other_race_count = total_count - (white_count + african_american_count + hispanic_count + asian_and_pacific_islander_count)
      campus_id = campuses_by_xid[campus_xid]['id']

      new_row = (
        campus_id,
        at_risk_count,
        economically_disadvantaged_count,
        gifted_and_talented_count,
        bilingual_and_esl_count,
        limited_english_proficient_count,
        non_educationally_disadvantaged_count,
        special_education_count,
        career_and_technical_education_count,
        mobility_count,
        white_count,
        african_american_count,
        hispanic_count,
        asian_and_pacific_islander_count,
        other_race_count,
        total_count
      )
      new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `students` ("
      "`campus_id`, "
      "`at_risk_count`, "
      "`economically_disadvantaged_count`, "
      "`gifted_and_talented_count`, "
      "`bilingual_and_esl_count`, "
      "`limited_english_proficient_count`, "
      "`non_educationally_disadvantaged_count`, "
      "`special_education_count`, "
      "`career_and_technical_education_count`, "
      "`mobility_count`, "
      "`white_count`, "
      "`african_american_count`, "
      "`hispanic_count`, "
      "`asian_and_pacific_islander_count`, "
      "`other_race_count`, "
      "`total_count`"
    ") VALUES ("
      "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`at_risk_count` = VALUES(`at_risk_count`), "
      "`economically_disadvantaged_count` = VALUES(`economically_disadvantaged_count`), "
      "`gifted_and_talented_count` = VALUES(`gifted_and_talented_count`), "
      "`bilingual_and_esl_count` = VALUES(`bilingual_and_esl_count`), "
      "`limited_english_proficient_count` = VALUES(`limited_english_proficient_count`), "
      "`non_educationally_disadvantaged_count` = VALUES(`non_educationally_disadvantaged_count`), "
      "`special_education_count` = VALUES(`special_education_count`), "
      "`career_and_technical_education_count` = VALUES(`career_and_technical_education_count`), "
      "`mobility_count` = VALUES(`mobility_count`), "
      "`white_count` = VALUES(`white_count`), "
      "`african_american_count` = VALUES(`african_american_count`), "
      "`hispanic_count` = VALUES(`hispanic_count`), "
      "`asian_and_pacific_islander_count` = VALUES(`asian_and_pacific_islander_count`), "
      "`other_race_count` = VALUES(`other_race_count`), "
      "`total_count` = VALUES(`total_count`)",
    new_rows
  )

def load_course_subject_areas_from_course_enrollment_file(csv_file, cursor):
  cursor.execute(
    "SELECT `name` FROM `course_subject_areas`"
  )
  rows = cursor.fetchall()
  course_subject_area_names = { row["name"]: row for row in rows }

  csv_reader = csv.DictReader(csv_file)

  new_rows = []
  for row in csv_reader:
    subject_area_name = row['SUBJAREAX']

    if subject_area_name not in course_subject_area_names:
      course_subject_area_names[subject_area_name] = { 'name': subject_area_name }

      new_row = (
        subject_area_name
      )
      new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `course_subject_areas` ("
      "`name`"
    ") VALUES ("
      "%s"
    ")",
    new_rows
  )

def load_course_subjects_from_course_enrollment_file(csv_file, course_subject_areas_by_name, cursor):
  cursor.execute(
    "SELECT `course_subject_area_id`, `name` FROM `course_subjects`"
  )
  rows = cursor.fetchall()
  course_subject_names = {}
  for row in rows:
    if row['course_subject_area_id'] not in course_subject_names:
      course_subject_names[row['course_subject_area_id']] = {}
    course_subject_names_for_area_id = course_subject_names[row['course_subject_area_id']]
    course_subject_names_for_area_id[row['name']] = row

  csv_reader = csv.DictReader(csv_file)

  new_rows = []
  for row in csv_reader:
    subject_area_name = row['SUBJAREAX']
    subject_name = row['SUBJECTX']

    subject_area_id = course_subject_areas_by_name[subject_area_name]['id']

    if subject_area_id not in course_subject_names:
      course_subject_names[subject_area_id] = {}
    course_subject_names_for_area_id = course_subject_names[subject_area_id]

    if subject_name not in course_subject_names_for_area_id:
      course_subject_names_for_area_id[subject_name] = { 'course_subject_area_id': subject_area_id, 'name': subject_name }

      new_row = (
        subject_area_id,
        subject_name
      )
      new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `course_subjects` ("
      "`course_subject_area_id`, "
      "`name`"
    ") VALUES ("
      "%s, %s"
    ")",
    new_rows
  )

def load_courses_from_course_enrollment_file(csv_file, school_year_id, course_subject_areas_by_name, course_subjects_by_area_id_and_name, grades_by_xid, cursor):
  cursor.execute(
    "SELECT `xid`  FROM `courses` WHERE `school_year_id` = %s", (school_year_id)
  )
  rows = cursor.fetchall()
  courses = { row["xid"]: row for row in rows }

  csv_reader = csv.DictReader(csv_file)

  new_rows = []
  for row in csv_reader:
    subject_area_name = row['SUBJAREAX']
    subject_name = row['SUBJECTX']
    course_xid = row['SERVICE']
    course_name = row['SERVICEX']
    grade_level_description = row['GRADE_LEVELX']

    grade_span_splits = [None, None]

    if grade_level_description == 'ALL GRADE LEVELS':
      grade_level_description = 'GRADES EE-12'
    elif grade_level_description == 'EARLY EDUCATION':
      grade_level_description = 'GRADES EE-EE'
    elif grade_level_description == 'PRE-KINDERGARTEN':
      grade_level_description = 'GRADES PK-PK'
    elif grade_level_description == 'KINDERGARTEN':
      grade_level_description = 'GRADES KG-KG'
    elif grade_level_description == 'PRE-KINDERGARTEN/KINDERGARTEN':
      grade_level_description = 'GRADES PK-KG'
    elif grade_level_description == 'KINDERGARTEN/ELEMENTARY (K-6)':
      grade_level_description = 'GRADES KG-06'
    elif grade_level_description == 'NOT APPLICABLE' or grade_level_description == 'UNKNOWN':
      grade_level_description = None

    if grade_level_description is not None:
      parentheses_grades_begin_index = grade_level_description.find('(GRADES ')
      grades_begin_index = grade_level_description.find('GRADES ')
      grade_begin_index = grade_level_description.find('GRADE ')

      if parentheses_grades_begin_index != -1:
        grade_span_begin_index = parentheses_grades_begin_index + len('(GRADES ')
        grade_span_end_index = grade_level_description.find(')', grade_span_begin_index)
      elif grades_begin_index != -1:
        grade_span_begin_index = grades_begin_index + len('GRADES ')
        grade_span_end_index = len(grade_level_description)
      elif grade_begin_index != -1:
        grade_span_begin_index = grade_begin_index + len('GRADE ')
        grade_span_end_index = len(grade_level_description)
      else:
        raise Exception('Unhandled grade span format: {0}'.format(grade_level_description))

      grade_span = grade_level_description[grade_span_begin_index:grade_span_end_index]

      splitter = '-'
      if ' - ' in grade_span:
        splitter = ' - '
      grade_span_splits = grade_span.split(splitter)

      if len(grade_span_splits) == 1:
        grade_span_splits.append(grade_span_splits[0])

      if len(grade_span_splits) != 2:
        raise Exception('Should have two grades in a grade span: {0}'.format(grade_level_description))

      grade_span_splits[0] = grade_span_splits[0].zfill(2)
      grade_span_splits[1] = grade_span_splits[1].zfill(2)

    min_grade_id = grades_by_xid[grade_span_splits[0]]['id'] if grade_span_splits[0] is not None else None
    max_grade_id = grades_by_xid[grade_span_splits[1]]['id'] if grade_span_splits[1] is not None else None

    subject_area_id = course_subject_areas_by_name[subject_area_name]['id']
    subject_id = course_subjects_by_area_id_and_name[subject_area_id][subject_name]['id']

    if course_xid not in courses:
      courses[course_xid] = { 'xid': course_xid }

      new_row = (
        school_year_id,
        course_xid,
        subject_id,
        course_name,
        min_grade_id,
        max_grade_id
      )
      new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `courses` ("
      "`school_year_id`, "
      "`xid`, "
      "`course_subject_id`, "
      "`name`, "
      "`min_grade_id`, "
      "`max_grade_id`"
    ") VALUES ("
      "%s, %s, %s, %s, %s, %s"
    ")",
    new_rows
  )

def load_course_enrollment_file(csv_file, campuses_by_xid, courses_by_xid, cursor):
  csv_reader = csv.DictReader(csv_file)

  new_rows = []
  for row in csv_reader:
    campus_xid = row['CAMPUS']
    course_xid = row['SERVICE']

    teacher_full_time_equivalent_count = None
    if row['TEACHER_FTE'] != '':
      teacher_full_time_equivalent_count = parse_numeric_round_to_1_decimal_place(row['TEACHER_FTE'])

    course_enrollment_count = parse_numeric_round_to_0_decimal_places(row['COURSE_ENROLL'])
    if course_enrollment_count == -99:
      course_enrollment_count = None

    if campus_xid in campuses_by_xid:
      campus_id = campuses_by_xid[campus_xid]['id']
      course_id = courses_by_xid[course_xid]['id']

      new_row = (
        campus_id,
        course_id,
        teacher_full_time_equivalent_count,
        course_enrollment_count
      )
      new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `course_enrollments` ("
      "`campus_id`, "
      "`course_id`, "
      "`teacher_full_time_equivalent_count`, "
      "`enrollment_count`"
    ") VALUES ("
      "%s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`teacher_full_time_equivalent_count` = VALUES(`teacher_full_time_equivalent_count`), "
      "`enrollment_count` = VALUES(`enrollment_count`)",
    new_rows
  )

def load_financial_fund_file(csv_file, data_format, school_year_id, school_year_end, cursor):
  csv_reader = csv.DictReader(csv_file)
  if data_format['has_header']:
    next(csv_reader)

  csv_reader.fieldnames = data_format['field_names']

  new_rows = []
  for row in csv_reader:
    if 'FUND' in data_format['field_names']:
      fund_xid = row['FUND']
      fund_description = row['FUNDX']
      fund_description_long = row['FUNDX_LONG']
    else:
      fund_xid = row['CS_NONPROF_ASSET']
      fund_description = row['CS_NONPROF_ASSETX']
      fund_description_long = row['CS_NONPROF_ASSETXL']

    new_row = (
      school_year_id,
      fund_xid,
      fund_description,
      fund_description_long
    )
    new_rows.append(new_row)

  # Missing for 2005
  if school_year_end == 2005:
    # Taken from 2006
    new_rows.append((school_year_id, "342", "SSA-TEACHER/PRIN TRNG RECRUIT", "SSA-TITLE II PART A: TEACHER AND PRINCIPAL TRAINING AND RECRUITING"))

  cursor.executemany(
    "INSERT INTO `financial_funds` ("
      "`school_year_id`, "
      "`xid`, "
      "`description`, "
      "`description_long`"
    ") VALUES ("
      "%s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`description` = VALUES(`description`), "
      "`description_long` = VALUES(`description_long`)",
    new_rows
  )

def load_financial_function_file(csv_file, data_format, school_year_id, cursor):
  csv_reader = csv.DictReader(csv_file)
  if data_format['has_header']:
    next(csv_reader)

  csv_reader.fieldnames = data_format['field_names']

  new_rows = []
  for row in csv_reader:
    if 'FUNCTION' in data_format['field_names']:
      function_xid = row['FUNCTION']
      function_description = row['FUNCTIONX']
      function_description_long = row['FUNCTIONX_LONG']
    else:
      function_xid = row['CS_NONPROF_FUNC']
      function_description = row['CS_NONPROF_FUNCX']
      function_description_long = row['CS_NONPROF_FUNCXL']

    new_row = (
      school_year_id,
      function_xid,
      function_description,
      function_description_long
    )
    new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `financial_functions` ("
      "`school_year_id`, "
      "`xid`, "
      "`description`, "
      "`description_long`"
    ") VALUES ("
      "%s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`description` = VALUES(`description`), "
      "`description_long` = VALUES(`description_long`)",
    new_rows
  )

def load_financial_object_file(csv_file, data_format, school_year_id, cursor):
  csv_reader = csv.DictReader(csv_file)
  if data_format['has_header']:
    next(csv_reader)

  csv_reader.fieldnames = data_format['field_names']

  new_rows = []
  for row in csv_reader:
    if 'OBJECT' in data_format['field_names']:
      object_xid = row['OBJECT']
      object_description = row['OBJECTX']
      object_description_long = row['OBJECTX_LONG']
    else:
      object_xid = row['CS_NONPROF_OBJ']
      object_description = row['CS_NONPROF_OBJX']
      object_description_long = row['CS_NONPROF_OBJXL']

    new_row = (
      school_year_id,
      object_xid,
      object_description,
      object_description_long
    )
    new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `financial_objects` ("
      "`school_year_id`, "
      "`xid`, "
      "`description`, "
      "`description_long`"
    ") VALUES ("
      "%s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`description` = VALUES(`description`), "
      "`description_long` = VALUES(`description_long`)",
    new_rows
  )

def load_financial_program_file(csv_file, data_format, school_year_id, cursor):
  csv_reader = csv.DictReader(csv_file)
  if data_format['has_header']:
    next(csv_reader)

  csv_reader.fieldnames = data_format['field_names']

  new_rows = []
  for row in csv_reader:
    program_xid = row['PROGRAM']
    program_description = row['PROGRAMX']
    program_description_long = row['PROGRAMX_LONG']

    new_row = (
      school_year_id,
      program_xid,
      program_description,
      program_description_long
    )
    new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `financial_programs` ("
      "`school_year_id`, "
      "`xid`, "
      "`description`, "
      "`description_long`"
    ") VALUES ("
      "%s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`description` = VALUES(`description`), "
      "`description_long` = VALUES(`description_long`)",
    new_rows
  )

def load_financial_program_intent_file(csv_file, data_format, school_year_id, cursor):
  csv_reader = csv.DictReader(csv_file)
  if data_format['has_header']:
    next(csv_reader)

  csv_reader.fieldnames = data_format['field_names']

  new_rows = []
  for row in csv_reader:
    if 'PROGRAM_INTENT' in data_format['field_names']:
      program_intent_xid = row['PROGRAM_INTENT']
      program_intent_description = row['PROGRAM_INTENTX']
      program_intent_description_long = row['PROGRAM_INTENTX_L']
    else:
      program_intent_xid = row['CS_NONPROF_PGMIN']
      program_intent_description = row['CS_NONPROF_PGMINX']
      program_intent_description_long = row['CS_NONPROF_PGMINXL']

    new_row = (
      school_year_id,
      program_intent_xid,
      program_intent_description,
      program_intent_description_long
    )
    new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `financial_program_intents` ("
      "`school_year_id`, "
      "`xid`, "
      "`description`, "
      "`description_long`"
    ") VALUES ("
      "%s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`description` = VALUES(`description`), "
      "`description_long` = VALUES(`description_long`)",
    new_rows
  )

def load_financial_unit_type_file(csv_file, data_format, school_year_id, cursor):
  csv_reader = csv.DictReader(csv_file)
  if data_format['has_header']:
    next(csv_reader)

  csv_reader.fieldnames = data_format['field_names']

  new_rows = []
  for row in csv_reader:
    unit_type_xid = row['FIN_UNIT_TYPE']
    unit_type_description = row['FIN_UNIT_TYPEX']
    unit_type_description_long = row['FIN_UNIT_TYPEX_LG']

    new_row = (
      school_year_id,
      unit_type_xid,
      unit_type_description,
      unit_type_description_long
    )
    new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `financial_unit_types` ("
      "`school_year_id`, "
      "`xid`, "
      "`description`, "
      "`description_long`"
    ") VALUES ("
      "%s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`description` = VALUES(`description`), "
      "`description_long` = VALUES(`description_long`)",
    new_rows
  )

def load_financial_unit_file(csv_file, data_format, school_year_id, school_year_end, districts_by_xid, financial_unit_types_by_xid, cursor):
  csv_reader = csv.DictReader(csv_file)
  if data_format['has_header']:
    next(csv_reader)

  csv_reader.fieldnames = data_format['field_names']

  new_rows = []
  for row in csv_reader:
    unit_xid = row['FIN_UNIT']
    unit_name = row['FIN_UNIT_NAME']
    unit_type_xid = row['FIN_UNIT_TYPE']
    district_xid = row['DISTRICT']

    # Education Service Center districts should be skipped. Their ID's end in "950".
    if district_xid[-3:] == '950':
      continue
    # Novice ISD closed after the 2012 school year, but the 2013 financial units still exist for it.
    # However, there is no accounting spending for any of these units, so we can skip them safely.
    if district_xid == '042906' and school_year_end == '2013':
      continue
    # Texas School for the Blind & Visually Impaired / Texas School for the Deaf
    # These aren't reported by the TEA for performance purposes, and the financial data stops
    # in 2013 anyway. We've decided to exclude them from all financial analysis for all years.
    if district_xid == '227905' or district_xid == '227906':
      continue

    if district_xid not in districts_by_xid:
      print("District {0} not in districts_by_xid. Skipping.".format(district_xid))
      continue

    district_id = districts_by_xid[district_xid]['id']
    unit_type_id = financial_unit_types_by_xid[unit_type_xid]['id']

    new_row = (
      school_year_id,
      district_id,
      unit_xid,
      unit_type_id,
      unit_name
    )
    new_rows.append(new_row)

  # # Missing for 2006
  # if school_year_end == 2006:
  #   # Taken from other districts in 2006
  #   new_rows.append((school_year_id, districts_by_xid["003904"]['id'], "998", financial_unit_types_by_xid["09"]['id'], "LOCAL UNALLOCATED"))
  #   new_rows.append((school_year_id, districts_by_xid["005902"]['id'], "699", financial_unit_types_by_xid["02"]['id'], "SUMMER SCHOOL PROGRAM"))
  #   new_rows.append((school_year_id, districts_by_xid["007902"]['id'], "699", financial_unit_types_by_xid["02"]['id'], "SUMMER SCHOOL PROGRAM"))
  #   new_rows.append((school_year_id, districts_by_xid["007904"]['id'], "699", financial_unit_types_by_xid["02"]['id'], "SUMMER SCHOOL PROGRAM"))

  cursor.executemany(
    "INSERT INTO `financial_units` ("
      "`school_year_id`, "
      "`district_id`, "
      "`xid`, "
      "`financial_unit_type_id`, "
      "`name`"
    ") VALUES ("
      "%s, %s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`financial_unit_type_id` = VALUES(`financial_unit_type_id`), "
      "`name` = VALUES(`name`)",
    new_rows
  )

def load_financial_account_file(csv_file, data_format, school_year_id, school_year_end, districts_by_xid, campuses_by_xid, financial_programs_by_xid, financial_program_intents_by_xid, financial_objects_by_xid, financial_functions_by_xid, financial_units_by_district_id_and_xid, financial_funds_by_xid, cursor):
  csv_reader = csv.DictReader(csv_file)
  if data_format['has_header']:
    next(csv_reader)

  csv_reader.fieldnames = data_format['field_names']

  linecount = 0
  new_rows = []
  for row in csv_reader:
    district_xid = row['DISTRICT']
    amount = row['ACTAMT']

    if 'FUND' in data_format['field_names']:
      fund_xid = row['FUND']
      function_xid = row['FUNCTION']
      object_xid = row['OBJECT']
      program_intent_xid = None
      if 'PROGRAM_INTENT' in row:
        program_intent_xid = row['PROGRAM_INTENT']
    else:
      fund_xid = row['CS_NONPROF_ASSET']
      function_xid = row['CS_NONPROF_FUNC']
      object_xid = row['CS_NONPROF_OBJ']
      program_intent_xid = None
      if 'CS_NONPROF_PGMIN' in row:
        program_intent_xid = row['CS_NONPROF_PGMIN']

    unit_xid = None
    if 'FIN_UNIT' in row:
      unit_xid = row['FIN_UNIT']

    campus_xid = None
    if 'CAMPUS' in row:
      campus_xid = '{0}{1}'.format(district_xid, row['CAMPUS'])
    elif unit_xid != 'NUL' and 1 <= int(unit_xid) <= 699:
      campus_xid = '{0}{1}'.format(district_xid, unit_xid)

    program_xid = None
    if 'PROGRAM' in row:
      program_xid = row['PROGRAM']

    # Education Service Center districts should be skipped. Their ID's end in "950".
    if district_xid[-3:] == '950':
      continue
    # Texas School for the Blind & Visually Impaired / Texas School for the Deaf
    # These aren't reported by the TEA for performance purposes, and the financial data stops
    # in 2013 anyway. We've decided to exclude them from all financial analysis for all years.
    if district_xid == '227905' or district_xid == '227906':
      continue

    if district_xid not in districts_by_xid:
      print("District {0} not in districts_by_xid. Skipping.".format(district_xid))
      continue

    district_id = districts_by_xid[district_xid]['id']
    fund_id = financial_funds_by_xid[fund_xid]['id']
    function_id = financial_functions_by_xid[function_xid]['id']

    object_id = None
    if object_xid is not None and object_xid in financial_objects_by_xid:
      object_id = financial_objects_by_xid[object_xid]['id']
    
    program_intent_id = None
    if program_intent_xid is not None:
      program_intent_id = financial_program_intents_by_xid[program_intent_xid]['id']

    program_id = None
    if program_xid is not None:
      program_id = financial_programs_by_xid[program_xid]['id']

    campus_id = None
    if campus_xid is not None and campus_xid in campuses_by_xid:
      campus_id = campuses_by_xid[campus_xid]['id']

    unit_id = None
    if unit_xid is not None and unit_xid != 'NUL' and unit_xid in financial_units_by_district_id_and_xid[district_id]:
        unit_id = financial_units_by_district_id_and_xid[district_id][unit_xid]['id']
    #else:
      #print("Missing financial unit. District XID: {0}, Unit XID: {1}".format(district_xid, unit_xid))
      #continue

    new_row = (
      school_year_id,
      district_id,
      campus_id,
      fund_id,
      function_id,
      object_id,
      unit_id,
      program_id,
      program_intent_id,
      amount
    )
    new_rows.append(new_row)

  cursor.executemany(
    "INSERT INTO `financial_accounts` ("
      "`school_year_id`, "
      "`district_id`, "
      "`campus_id`, "
      "`financial_fund_id`, "
      "`financial_function_id`, "
      "`financial_object_id`, "
      "`financial_unit_id`, "
      "`financial_program_id`, "
      "`financial_program_intent_id`, "
      "`amount`"
    ") VALUES ("
      "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
    ") ON DUPLICATE KEY UPDATE"
      "`amount` = VALUES(`amount`)",
    new_rows
  )

def load_districts(first_school_year_end, last_school_year_end):
  conn = connect(db=default_db)

  try:
    with conn.cursor() as cursor:

      cursor.execute(
        "SELECT `id`, `end_year` FROM `school_years`"
      )
      rows = cursor.fetchall()
      school_years_by_end_year = { row["end_year"]: row for row in rows }

      aeis_first_school_year_end = max(2004, first_school_year_end)
      aeis_last_school_year_end = min(2012, last_school_year_end)
      for school_year_end in range(aeis_first_school_year_end, aeis_last_school_year_end + 1):
        print('Processing AEIS DREF for year {0}'.format(school_year_end))
        school_year_id = school_years_by_end_year[school_year_end]['id']
        with open(get_aeis_dref_csv(year=school_year_end), newline='') as csv_file:
          load_district_reference_file(
            csv_file=csv_file,
            school_year_id=school_year_id,
            cursor=cursor)

      tapr_first_school_year_end = max(first_school_year_end, 2013)
      tapr_last_school_year_end = min(last_school_year_end, 2016)
      for school_year_end in range(tapr_first_school_year_end, tapr_last_school_year_end + 1):
        print('Processing TAPR DREF for year {0}'.format(school_year_end))
        school_year_id = school_years_by_end_year[school_year_end]['id']
        with open(get_tapr_dref_csv(year=school_year_end), newline='') as csv_file:
          load_district_reference_file(
            csv_file=csv_file,
            school_year_id=school_year_id,
            cursor=cursor)

    conn.commit()
  finally:
    conn.close()

def get_district_maps(school_year_id, cursor):
  cursor.execute(
    "SELECT `id`, `xid`, `name`, `county_xid`, `region_xid` FROM `districts` WHERE `school_year_id` = %s", (school_year_id)
  )
  rows = cursor.fetchall()
  districts_by_xid = { row["xid"]: row for row in rows }

  districts_by_other_data = { }
  for row in rows:
    if row['region_xid'] not in districts_by_other_data:
      districts_by_other_data[row['region_xid']] = {}
    region = districts_by_other_data[row['region_xid']]

    if row['county_xid'] not in region:
      region[row['county_xid']] = {}
    county = region[row['county_xid']]

    if row['name'] not in county:
      county[row['name']] = row
    else:
      raise Exception('Two districts with the same name were found in one county.')

  return (districts_by_xid, districts_by_other_data)

def load_campuses(first_school_year_end, last_school_year_end):
  conn = connect(db=default_db)

  try:
    with conn.cursor() as cursor:

      cursor.execute(
        "SELECT `id`, `end_year` FROM `school_years`"
      )
      rows = cursor.fetchall()
      school_years_by_end_year = { row["end_year"]: row for row in rows }

      cursor.execute(
        "SELECT `id`, `xid` FROM `grades`"
      )
      rows = cursor.fetchall()
      grades_by_xid = { row["xid"]: row for row in rows }

      cursor.execute(
        "SELECT `id`, `xid` FROM `grade_types`"
      )
      rows = cursor.fetchall()
      grade_types_by_xid = { row["xid"]: row for row in rows }

      aeis_first_school_year_end = max(2004, first_school_year_end)
      aeis_last_school_year_end = min(2012, last_school_year_end)
      for school_year_end in range(aeis_first_school_year_end, aeis_last_school_year_end + 1):
        print('Processing AEIS CREF for year {0}'.format(school_year_end))
        school_year_id = school_years_by_end_year[school_year_end]['id']
        districts_by_xid, districts_by_other_data = get_district_maps(school_year_id=school_year_id, cursor=cursor)
        with open(get_aeis_cref_csv(year=school_year_end), newline='') as csv_file:
          load_campus_reference_file(
            csv_file=csv_file,
            school_year_id=school_year_id,
            districts_by_xid=districts_by_xid,
            districts_by_other_data=districts_by_other_data,
            grades_by_xid=grades_by_xid,
            grade_types_by_xid=grade_types_by_xid,
            cursor=cursor)

      tapr_first_school_year_end = max(first_school_year_end, 2013)
      tapr_last_school_year_end = min(last_school_year_end, 2016)
      for school_year_end in range(tapr_first_school_year_end, tapr_last_school_year_end + 1):
        print('Processing TAPR CREF for year {0}'.format(school_year_end))
        school_year_id = school_years_by_end_year[school_year_end]['id']
        districts_by_xid, districts_by_other_data = get_district_maps(school_year_id=school_year_id, cursor=cursor)
        with open(get_tapr_cref_csv(year=school_year_end), newline='') as csv_file:
          load_campus_reference_file(
            csv_file=csv_file,
            school_year_id=school_year_id,
            districts_by_xid=districts_by_xid,
            districts_by_other_data=districts_by_other_data,
            grades_by_xid=grades_by_xid,
            grade_types_by_xid=grade_types_by_xid,
            cursor=cursor)

    conn.commit()
  finally:
    conn.close()

def load_staff(first_school_year_end, last_school_year_end):
  conn = connect(db=default_db)

  try:
    with conn.cursor() as cursor:

      cursor.execute(
        "SELECT `id`, `end_year` FROM `school_years`"
      )
      rows = cursor.fetchall()
      school_years_by_end_year = { row["end_year"]: row for row in rows }

      aeis_first_school_year_end = max(2004, first_school_year_end)
      aeis_last_school_year_end = min(2012, last_school_year_end)
      for school_year_end in range(aeis_first_school_year_end, aeis_last_school_year_end + 1):
        print('Processing AEIS CSTAF for year {0}'.format(school_year_end))

        cursor.execute(
          "SELECT `id`, `xid` FROM `campuses` WHERE `school_year_id` = %s", (school_years_by_end_year[school_year_end]['id'])
        )
        rows = cursor.fetchall()
        campuses_by_xid = { row["xid"]: row for row in rows }

        with open(get_aeis_cstaf_csv(year=school_year_end), newline='') as csv_file:
          load_staff_reference_file(
            csv_file=csv_file,
            campuses_by_xid=campuses_by_xid,
            cursor=cursor)

      tapr_first_school_year_end = max(first_school_year_end, 2013)
      tapr_last_school_year_end = min(last_school_year_end, 2016)
      for school_year_end in range(tapr_first_school_year_end, tapr_last_school_year_end + 1):
        print('Processing TAPR CPROF (staff only) for year {0}'.format(school_year_end))

        cursor.execute(
          "SELECT `id`, `xid` FROM `campuses` WHERE `school_year_id` = %s", (school_years_by_end_year[school_year_end]['id'])
        )
        rows = cursor.fetchall()
        campuses_by_xid = { row["xid"]: row for row in rows }

        with open(get_tapr_cprof_csv(year=school_year_end), newline='') as csv_file:
          load_staff_reference_file(
            csv_file=csv_file,
            campuses_by_xid=campuses_by_xid,
            cursor=cursor)

    conn.commit()
  finally:
    conn.close()

def load_students(first_school_year_end, last_school_year_end):
  conn = connect(db=default_db)

  try:
    with conn.cursor() as cursor:

      cursor.execute(
        "SELECT `id`, `end_year` FROM `school_years`"
      )
      rows = cursor.fetchall()
      school_years_by_end_year = { row["end_year"]: row for row in rows }

      aeis_first_school_year_end = max(2004, first_school_year_end)
      aeis_last_school_year_end = min(2012, last_school_year_end)
      for school_year_end in range(aeis_first_school_year_end, aeis_last_school_year_end + 1):
        print('Processing AEIS CSTUD for year {0}'.format(school_year_end))

        cursor.execute(
          "SELECT `id`, `xid` FROM `campuses` WHERE `school_year_id` = %s", (school_years_by_end_year[school_year_end]['id'])
        )
        rows = cursor.fetchall()
        campuses_by_xid = { row["xid"]: row for row in rows }

        with open(get_aeis_cstud_csv(year=school_year_end), newline='') as csv_file:
          load_student_reference_file(
            csv_file=csv_file,
            campuses_by_xid=campuses_by_xid,
            cursor=cursor)

      tapr_first_school_year_end = max(first_school_year_end, 2013)
      tapr_last_school_year_end = min(last_school_year_end, 2016)
      for school_year_end in range(tapr_first_school_year_end, tapr_last_school_year_end + 1):
        print('Processing TAPR CPROF (student only) for year {0}'.format(school_year_end))

        cursor.execute(
          "SELECT `id`, `xid` FROM `campuses` WHERE `school_year_id` = %s", (school_years_by_end_year[school_year_end]['id'])
        )
        rows = cursor.fetchall()
        campuses_by_xid = { row["xid"]: row for row in rows }

        with open(get_tapr_cprof_csv(year=school_year_end), newline='') as csv_file:
          load_student_reference_file(
            csv_file=csv_file,
            campuses_by_xid=campuses_by_xid,
            cursor=cursor)

    conn.commit()
  finally:
    conn.close()

def load_course_enrollments(first_school_year_end, last_school_year_end):
  conn = connect(db=default_db)

  try:
    with conn.cursor() as cursor:

      cursor.execute(
        "SELECT `id`, `end_year` FROM `school_years`"
      )
      rows = cursor.fetchall()
      school_years_by_end_year = { row["end_year"]: row for row in rows }

      cursor.execute(
        "SELECT `id`, `xid` FROM `grades`"
      )
      rows = cursor.fetchall()
      grades_by_xid = { row["xid"]: row for row in rows }

      for school_year_end in range(first_school_year_end, last_school_year_end + 1):
        print('Processing course data for year {0}'.format(school_year_end))

        csv = get_course_enrollment_csv(year=school_year_end)

        school_year_id = school_years_by_end_year[school_year_end]['id']

        with open(csv, newline='') as csv_file:
          load_course_subject_areas_from_course_enrollment_file(
            csv_file=csv_file,
            cursor=cursor)

        cursor.execute(
          "SELECT `id`, `name` FROM `course_subject_areas`"
        )
        rows = cursor.fetchall()
        course_subject_areas_by_name = { row["name"]: row for row in rows }

        with open(csv, newline='') as csv_file:
          load_course_subjects_from_course_enrollment_file(
            csv_file=csv_file,
            course_subject_areas_by_name=course_subject_areas_by_name,
            cursor=cursor)

        cursor.execute(
          "SELECT `id`, `course_subject_area_id`, `name` FROM `course_subjects`"
        )
        rows = cursor.fetchall()
        course_subjects_by_area_id_and_name = {}
        for row in rows:
          if row['course_subject_area_id'] not in course_subjects_by_area_id_and_name:
            course_subjects_by_area_id_and_name[row['course_subject_area_id']] = {}
          course_subjects_for_area_id = course_subjects_by_area_id_and_name[row['course_subject_area_id']]
          course_subjects_for_area_id[row['name']] = row

        with open(csv, newline='') as csv_file:
          load_courses_from_course_enrollment_file(
            csv_file=csv_file,
            school_year_id=school_year_id,
            course_subject_areas_by_name=course_subject_areas_by_name,
            course_subjects_by_area_id_and_name=course_subjects_by_area_id_and_name,
            grades_by_xid=grades_by_xid,
            cursor=cursor)

        cursor.execute(
          "SELECT `id`, `xid` FROM `campuses` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        campuses_by_xid = { row["xid"]: row for row in rows }

        cursor.execute(
          "SELECT `id`, `xid` FROM `courses` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        courses_by_xid = { row["xid"]: row for row in rows }

        with open(csv, newline='') as csv_file:
          load_course_enrollment_file(
            csv_file=csv_file,
            campuses_by_xid=campuses_by_xid,
            courses_by_xid=courses_by_xid,
            cursor=cursor)

    conn.commit()
  finally:
    conn.close()

def load_financials(first_school_year_end, last_school_year_end):
  conn = connect(db=default_db)

  try:
    with conn.cursor() as cursor:

      cursor.execute(
        "SELECT `id`, `end_year` FROM `school_years`"
      )
      rows = cursor.fetchall()
      school_years_by_end_year = { row["end_year"]: row for row in rows }

      for school_year_end in range(first_school_year_end, last_school_year_end + 1):
        print('Unzipping financial data for year {0}'.format(school_year_end))
        extract_financial_zip(year=school_year_end)

      for school_year_end in range(first_school_year_end, last_school_year_end + 1):
        print('Processing financial data for year {0}'.format(school_year_end))

        school_year_id = school_years_by_end_year[school_year_end]['id']

        cursor.execute(
          "SELECT `id`, `xid` FROM `districts` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        districts_by_xid = { row["xid"]: row for row in rows }

        cursor.execute(
          "SELECT `id`, `xid` FROM `campuses` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        campuses_by_xid = { row["xid"]: row for row in rows }

        with open(get_financial_fund_csv(year=school_year_end), newline='') as csv_file:
          load_financial_fund_file(
            csv_file=csv_file,
            data_format=get_financial_fund_data_format(year=school_year_end),
            school_year_id=school_year_id,
            school_year_end=school_year_end,
            cursor=cursor)

        with open(get_financial_charter_asset_csv(year=school_year_end), newline='') as csv_file:
          load_financial_fund_file(
            csv_file=csv_file,
            data_format=get_financial_charter_asset_data_format(year=school_year_end),
            school_year_id=school_year_id,
            school_year_end=school_year_end,
            cursor=cursor)

        with open(get_financial_program_csv(year=school_year_end), newline='') as csv_file:
          load_financial_program_file(
            csv_file=csv_file,
            data_format=get_financial_program_data_format(year=school_year_end),
            school_year_id=school_year_id,
            cursor=cursor)

        program_intent_csv = get_financial_program_intent_csv(year=school_year_end)
        if program_intent_csv is not None:
          with open(program_intent_csv, newline='') as csv_file:
            load_financial_program_intent_file(
              csv_file=csv_file,
              data_format=get_financial_program_intent_data_format(year=school_year_end),
              school_year_id=school_year_id,
              cursor=cursor)

        charter_program_intent_csv = get_financial_charter_program_intent_csv(year=school_year_end)
        if charter_program_intent_csv is not None:
          with open(charter_program_intent_csv, newline='') as csv_file:
            load_financial_program_intent_file(
              csv_file=csv_file,
              data_format=get_financial_charter_program_intent_data_format(year=school_year_end),
              school_year_id=school_year_id,
              cursor=cursor)

        with open(get_financial_object_csv(year=school_year_end), newline='') as csv_file:
          load_financial_object_file(
            csv_file=csv_file,
            data_format=get_financial_object_data_format(year=school_year_end),
            school_year_id=school_year_id,
            cursor=cursor)

        with open(get_financial_charter_object_csv(year=school_year_end), newline='') as csv_file:
          load_financial_object_file(
            csv_file=csv_file,
            data_format=get_financial_charter_object_data_format(year=school_year_end),
            school_year_id=school_year_id,
            cursor=cursor)

        with open(get_financial_function_csv(year=school_year_end), newline='') as csv_file:
          load_financial_function_file(
            csv_file=csv_file,
            data_format=get_financial_function_data_format(year=school_year_end),
            school_year_id=school_year_id,
            cursor=cursor)

        with open(get_financial_charter_function_csv(year=school_year_end), newline='') as csv_file:
          load_financial_function_file(
            csv_file=csv_file,
            data_format=get_financial_charter_function_data_format(year=school_year_end),
            school_year_id=school_year_id,
            cursor=cursor)

        with open(get_financial_unit_type_csv(year=school_year_end), newline='') as csv_file:
          load_financial_unit_type_file(
            csv_file=csv_file,
            data_format=get_financial_unit_type_data_format(year=school_year_end),
            school_year_id=school_year_id,
            cursor=cursor)

        cursor.execute(
          "SELECT `id`, `xid` FROM `financial_unit_types` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        financial_unit_types_by_xid = { row["xid"]: row for row in rows }

        unit_csv = get_financial_unit_csv(year=school_year_end)
        if unit_csv is not None:
          with open(unit_csv, newline='') as csv_file:
            load_financial_unit_file(
              csv_file=csv_file,
              data_format=get_financial_unit_data_format(year=school_year_end),
              school_year_id=school_year_id,
              school_year_end=school_year_end,
              districts_by_xid=districts_by_xid,
              financial_unit_types_by_xid=financial_unit_types_by_xid,
              cursor=cursor)

        cursor.execute(
          "SELECT `id`, `xid` FROM `financial_programs` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        financial_programs_by_xid = { row["xid"]: row for row in rows }

        cursor.execute(
          "SELECT `id`, `xid` FROM `financial_program_intents` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        financial_program_intents_by_xid = { row["xid"]: row for row in rows }

        cursor.execute(
          "SELECT `id`, `xid` FROM `financial_objects` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        financial_objects_by_xid = { row["xid"]: row for row in rows }

        cursor.execute(
          "SELECT `id`, `xid` FROM `financial_functions` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        financial_functions_by_xid = { row["xid"]: row for row in rows }

        cursor.execute(
          "SELECT `id`, `district_id`, `xid` FROM `financial_units` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        financial_units_by_district_id_and_xid = {}
        for row in rows:
          if row['district_id'] not in financial_units_by_district_id_and_xid:
            financial_units_by_district_id_and_xid[row['district_id']] = {}
          financial_units_for_district_by_xid = financial_units_by_district_id_and_xid[row['district_id']]
          financial_units_for_district_by_xid[row['xid']] = row

        cursor.execute(
          "SELECT `id`, `xid` FROM `financial_funds` WHERE `school_year_id` = %s", (school_year_id)
        )
        rows = cursor.fetchall()
        financial_funds_by_xid = { row["xid"]: row for row in rows }

        split_account_csvs = get_financial_account_csvs(year=school_year_end)
        with fileinput.input(files=split_account_csvs) as csv_file:
          load_financial_account_file(
            csv_file=csv_file,
            data_format=get_financial_account_data_format(year=school_year_end),
            school_year_id=school_year_id,
            school_year_end=school_year_end,
            districts_by_xid=districts_by_xid,
            campuses_by_xid=campuses_by_xid,
            financial_programs_by_xid=financial_programs_by_xid,
            financial_program_intents_by_xid=financial_program_intents_by_xid,
            financial_objects_by_xid=financial_objects_by_xid,
            financial_functions_by_xid=financial_functions_by_xid,
            financial_units_by_district_id_and_xid=financial_units_by_district_id_and_xid,
            financial_funds_by_xid=financial_funds_by_xid,
            cursor=cursor)

        split_charter_account_csvs = get_financial_charter_account_csvs(year=school_year_end)
        with fileinput.input(files=split_charter_account_csvs) as csv_file:
          load_financial_account_file(
            csv_file=csv_file,
            data_format=get_financial_charter_account_data_format(year=school_year_end),
            school_year_id=school_year_id,
            school_year_end=school_year_end,
            districts_by_xid=districts_by_xid,
            campuses_by_xid=campuses_by_xid,
            financial_programs_by_xid=financial_programs_by_xid,
            financial_program_intents_by_xid=financial_program_intents_by_xid,
            financial_objects_by_xid=financial_objects_by_xid,
            financial_functions_by_xid=financial_functions_by_xid,
            financial_units_by_district_id_and_xid=financial_units_by_district_id_and_xid,
            financial_funds_by_xid=financial_funds_by_xid,
            cursor=cursor)

        # Commit after each year has been loaded
        conn.commit()
  finally:
    conn.close()



##########
# Uncomment each line below as needed. Leaving the lines commented makes execution much faster for the thing you're trying to do.
##########

# Use this line to add to the DB schema or drop/create it. Be careful what you do here.

#create_db()


# Use these lines to load data into the schema. The year ranges are inclusive of the boundaries.
# Specify only one year for the start and end (e.g., 2016, 2016) if you want to just load/reload
# a single year at a time, which makes the runtimes much saner.

#load_districts(2004, 2016)
#load_campuses(2004, 2016)
#load_staff(2004, 2016)
#load_students(2004, 2016)
#load_course_enrollments(2005, 2016)
#load_financials(2016, 2016)
