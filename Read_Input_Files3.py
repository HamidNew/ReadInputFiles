# from csv import reader
import csv
import datetime
import os
import pandas as pd


# Processing CSV files
input_csv_file    = 'Deal_List.csv'
output_csv_file   = 'Output_File_01.csv'
output_err_file   = 'Error_File_01.txt'
output_par_file   = 'Output_File_01.parquet'

Country_csv_file  = 'Country_List.csv'
Currency_csv_file = 'Currency_List.csv'
Company_csv_file  = 'Company_List.csv'

df_Country  = pd.read_csv(Country_csv_file)
df_Currency = pd.read_csv(Currency_csv_file)
df_Company  = pd.read_csv(Company_csv_file)

df = pd.read_csv(input_csv_file)

# RowHash
def createRowHash(df):
    try:
        df = df.drop('hash', 1) # lose the old hash
    except:
        pass
    df['hash'] = pd.Series((hash(tuple(row)) for _, row in df.iterrows()))

createRowHash(df)
df = df.fillna("")
# print(df)

output_file = open(output_csv_file, 'w')
output_writer = csv.writer(output_file)

error_file = open(output_err_file, 'w')

header = ['RowNo','Deal Name','D1','D2','D3','D4','D5','Is Active','Country','Currency','Company','Company Name','AsOfDate','ProcessIdentifier','RowHash']
output_writer.writerow(header)

error_messages = []

# Input CSV rows process
for RowNo, row in df.iterrows():

    # Deal_Name
    Deal_Name = row['Deal Name']
    if len(Deal_Name) == 0:
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - Missing Deal Name, it is a Mandatory column.'.format(RowNo+1,'Deal Name',Deal_Name)
        print(msg)
        error_messages.append(msg)

    # D1 to D5
    Ds = []
    for v in range(1,6):
        col = 'D'+str(v)
        val = str(row[col])
        val = val.strip()
        try:
            f = float(val)
        except:
            msg = 'RowNo: {:06}: Column: {} Value: "{}" - only Decimal/Float is allowed'.format(RowNo+1,col,val)
            print(msg)
            error_messages.append(msg)
            f = 0
        Ds.append(f)

    if Ds[0] == 0 and Ds[1] == 0 and Ds[2] == 0 and Ds[3] == 0 and Ds[4] == 0:
        msg = 'RowNo: {:06}: - D1-D5 are all empty/invalid, need atleast one Decimal value'.format(RowNo+1)
        print(msg)
        error_messages.append(msg)

    # Is_Active
    Is_Active = row['Is Active'].strip().upper()
    if Is_Active != "YES" and Is_Active != "NO":
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - only Yes/No is allowed'.format(RowNo+1,'Is_Active',Is_Active)
        print(msg)
        error_messages.append(msg)

    # Country
    Country = row['Country'].strip()
    df_tmp = df_Country[(df_Country.Code == Country)]
    rows = len(df_tmp.axes[0])
    Country_Name = ''
    if rows == 0:
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Country code'.format(RowNo+1,'Country',Country)
        print(msg)
        error_messages.append(msg)
    else:
        for _, rowa in df_tmp.iterrows():
            Country_Name = rowa['Name']

    # Currency
    Currency = row['Currency'].strip()
    df_tmp = df_Currency[(df_Currency.Code == Currency)]
    rows = len(df_tmp.axes[0])
    Currency_Name = ''
    if rows == 0:
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Currency code'.format(RowNo+1,'Currency',Currency)
        print(msg)
        error_messages.append(msg)
    else:
        for _, rowa in df_tmp.iterrows():
            Currency_Name = rowa['Name']

    # Company
    Company = row['Company']
    Company_Name = ''
    if isinstance(Company, float):
        df_tmp = df_Company[(df_Company.Id == Company)]
        rows = len(df_tmp.axes[0])
        if rows == 0:
            msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Company code'.format(RowNo+1,'Company',int(Company))
            print(msg)
            error_messages.append(msg)
        else:
            for _, rowa in df_tmp.iterrows():
                Company_Name = rowa['Name']
    else:
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Company code'.format(RowNo+1,'Company',Company)
        print(msg)
        error_messages.append(msg)

    # AsOfDate
    AsOfDate = datetime.datetime.now()

    # ProcessIdentifier
    ProcessIdentifier = os.getpid()

    # Output
    output_row = [
        RowNo+1,row['Deal Name'],
        row['D1'],row['D2'],row['D3'],row['D4'],row['D5'],
        row['Is Active'],row['Country'],row['Currency'],
        row['Company'],Company_Name,AsOfDate,ProcessIdentifier,row['hash']]
    output_writer.writerow(output_row)

for row in error_messages:
    print(row)
    error_file.write(row+'\n')
############################################################################


# Processing EXCEL files
input_excel_codes_file = './Deal_List_Lookup_Codes.xlsx'
df_Country  = pd.read_excel(input_excel_codes_file, sheet_name='Country')
df_Currency = pd.read_excel(input_excel_codes_file, sheet_name='Currency')
df_Company  = pd.read_excel(input_excel_codes_file, sheet_name='Company')

input_excel_file = './Deal_List.xlsx'
df = pd.read_excel(input_excel_file)

# RowHash
createRowHash(df)

df = df.fillna("")
print(df)


# Input EXCEL rows process
for RowNo, row in df.iterrows():

    # Deal_Name
    Deal_Name = row['Deal Name']
    if len(Deal_Name) == 0:
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - Missing Deal Name, it is a Mandatory column.'.format(RowNo+1,'Deal Name',Deal_Name)
        print(msg)
        error_messages.append(msg)

    # D1 to D5
    Ds = []
    for v in range(1,6):
        col = 'D'+str(v)
        val = str(row[col])
        val = val.strip()
        try:
            f = float(val)
        except:
            msg = 'RowNo: {:06}: Column: {} Value: "{}" - only Decimal/Float is allowed'.format(RowNo+1,col,val)
            print(msg)
            error_messages.append(msg)
            f = 0
        Ds.append(f)

    if Ds[0] == 0 and Ds[1] == 0 and Ds[2] == 0 and Ds[3] == 0 and Ds[4] == 0:
        msg = 'RowNo: {:06}: - D1-D5 are all empty/invalid, need atleast one Decimal value'.format(RowNo+1)
        print(msg)
        error_messages.append(msg)

    # Is_Active
    Is_Active = row['Is Active'].strip().upper()
    if Is_Active != "YES" and Is_Active != "NO":
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - only Yes/No is allowed'.format(RowNo+1,'Is_Active',Is_Active)
        print(msg)
        error_messages.append(msg)

    # Country
    Country = row['Country'].strip()
    df_tmp = df_Country[(df_Country.Code == Country)]
    rows = len(df_tmp.axes[0])
    Country_Name = ''
    if rows == 0:
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Country code'.format(RowNo+1,'Country',Country)
        print(msg)
        error_messages.append(msg)
    else:
        for _, rowa in df_tmp.iterrows():
            Country_Name = rowa['Name']

    # Currency
    Currency = row['Currency'].strip()
    df_tmp = df_Currency[(df_Currency.Code == Currency)]
    rows = len(df_tmp.axes[0])
    Currency_Name = ''
    if rows == 0:
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Currency code'.format(RowNo+1,'Currency',Currency)
        print(msg)
        error_messages.append(msg)
    else:
        for _, rowa in df_tmp.iterrows():
            Currency_Name = rowa['Name']

    # Company
    Company = row['Company']
    Company_Name = ''
    if isinstance(Company, float):
        df_tmp = df_Company[(df_Company.Id == Company)]
        rows = len(df_tmp.axes[0])
        if rows == 0:
            msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Company code'.format(RowNo+1,'Company',int(Company))
            print(msg)
            error_messages.append(msg)
        else:
            for _, rowa in df_tmp.iterrows():
                Company_Name = rowa['Name']
    else:
        msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Company code'.format(RowNo+1,'Company',Company)
        print(msg)
        error_messages.append(msg)


    # AsOfDate
    AsOfDate = datetime.datetime.now()

    # ProcessIdentifier
    ProcessIdentifier = os.getpid()

    # Output
    output_row = [
        RowNo+1,row['Deal Name'],
        row['D1'],row['D2'],row['D3'],row['D4'],row['D5'],
        row['Is Active'],row['Country'],row['Currency'],
        row['Company'],Company_Name,AsOfDate,ProcessIdentifier,row['hash']]
    output_writer.writerow(output_row)

for row in error_messages:
    print(row)
    error_file.write(row+'\n')

output_file.close()
error_file.close()

df = pd.read_csv(output_csv_file)
df.to_parquet(output_par_file)
