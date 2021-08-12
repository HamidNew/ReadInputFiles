# from csv import reader
import csv
import datetime
import os
import pandas as pd

class Deal():
    def __init__(self):
        self.AsOfDate	            =	""
        self.Company	            =	""
        self.Company_csv_file	    =	""
        self.Company_Name	        =	""
        self.Country	            =	""
        self.Country_csv_file	    =	""
        self.Country_Name	        =	""
        self.Currency	            =	""
        self.Currency_csv_file	    =	""
        self.Currency_Name	        =	""
        self.Deal_Name	            =	""
        self.df	                    =	""
        self.df_Company	            =	""
        self.df_Country	            =	""
        self.df_Currency	        =	""
        self.df_tmp	                =	""
        self.Ds	                    =	""
        self.error_file	            =	""
        self.error_messages	        =	""
        self.header	                =	""
        self.input_csv_file	        =	""
        self.input_excel_codes_file	=	""
        self.input_excel_file	    =	""
        self.Is_Active	            =	""
        self.msg	                =	""
        self.output_csv_file	    =	""
        self.output_err_file	    =	""
        self.output_file	        =	""
        self.output_par_file	    =	""
        self.output_row	            =	""
        self.output_writer	        =	""
        self.ProcessIdentifier	    =	""
        self.row	                =	""
        self.RowNo	                =	""
        self.rows	                =	""

    def loadCsvFiles(self):
        # Processing CSV files
        self.input_csv_file    = 'Deal_List.csv'
        self.output_csv_file   = 'Output_File_01.csv'
        self.Country_csv_file  = 'Country_List.csv'
        self.Currency_csv_file = 'Currency_List.csv'
        self.Company_csv_file  = 'Company_List.csv'

        self.df_Country  = pd.read_csv(self.Country_csv_file)
        self.df_Currency = pd.read_csv(self.Currency_csv_file)
        self.df_Company  = pd.read_csv(self.Company_csv_file)
        self.df          = pd.read_csv(self.input_csv_file)
        self.output_file = open(self.output_csv_file, 'w')
        self.output_writer = csv.writer(self.output_file)

    def loadErrParFiles(self):
        # Processing Error and Parquet files
        self.output_err_file   = 'Error_File_01.txt'
        self.output_par_file   = 'Output_File_01.parquet'
        self.df = pd.read_csv(self.input_csv_file)
        # self.df = self.df.fillna("")
        self.error_file = open(self.output_err_file, 'w')
        self.error_messages = []

    def loadExcelFiles(self):
        # Processing EXCEL files
        self.input_excel_codes_file = './Deal_List_Lookup_Codes.xlsx'
        self.df_Country  = pd.read_excel(self.input_excel_codes_file, sheet_name='Country')
        self.df_Currency = pd.read_excel(self.input_excel_codes_file, sheet_name='Currency')
        self.df_Company  = pd.read_excel(self.input_excel_codes_file, sheet_name='Company')

        self.input_excel_file = './Deal_List.xlsx'
        self.df = pd.read_excel(self.input_excel_file)
        # self.df = self.df.fillna("")
        print(self.df)

    def createRowHash(self):
        # RowHash
        try:
            self.df = self.df.drop('hash', 1) # lose the old hash
        except:
            pass
        self.df['hash'] = pd.Series((hash(tuple(row)) for _, row in self.df.iterrows()))
        self.df = self.df.fillna("")
        print(self.df)

    def writeHeader(self):
        # Header
        self.header = ['RowNo','Deal Name','D1','D2','D3','D4','D5','Is Active','Country','Currency','Company','Company Name','AsOfDate','ProcessIdentifier','RowHash']
        self.output_writer.writerow(self.header)

    def checkDeal_Name(self):
        # Deal_Name
        self.Deal_Name = self.row['Deal Name']
        if len(self.Deal_Name) == 0:
            self.msg = 'RowNo: {:06}: Column: {} Value: "{}" - Missing Deal Name, it is a Mandatory column.'.format(self.RowNo+1,'Deal Name',self.Deal_Name)
            print(self.msg)
            self.error_messages.append(self.msg)

    def checkDs(self):
        # D1 to D5
        self.Ds = []
        for v in range(1,6):
            col = 'D'+str(v)
            val = str(self.row[col])
            val = val.strip()
            try:
                f = float(val)
            except:
                self.msg = 'RowNo: {:06}: Column: {} Value: "{}" - only Decimal/Float is allowed'.format(self.RowNo+1,col,val)
                print(self.msg)
                self.error_messages.append(self.msg)
                f = 0
            self.Ds.append(f)

        if self.Ds[0] == 0 and self.Ds[1] == 0 and self.Ds[2] == 0 and self.Ds[3] == 0 and self.Ds[4] == 0:
            self.msg = 'RowNo: {:06}: - D1-D5 are all empty/invalid, need atleast one Decimal value'.format(self.RowNo+1)
            print(self.msg)
            self.error_messages.append(self.msg)

    def checkIs_Active(self):
        # Is_Active
        self.Is_Active = self.row['Is Active'].strip().upper()
        if self.Is_Active != "YES" and self.Is_Active != "NO":
            self.msg = 'RowNo: {:06}: Column: {} Value: "{}" - only Yes/No is allowed'.format(self.RowNo+1,'Is_Active',self.Is_Active)
            print(self.msg)
            self.error_messages.append(self.msg)

    def checkCountry(self):
        # Country
        self.Country = self.row['Country'].strip()
        self.df_tmp = self.df_Country[(self.df_Country.Code == self.Country)]
        self.rows = len(self.df_tmp.axes[0])
        self.Country_Name = ''
        if self.rows == 0:
            self.msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Country code'.format(self.RowNo+1,'Country',self.Country)
            print(self.msg)
            self.error_messages.append(self.msg)
        else:
            for _, rowa in self.df_tmp.iterrows():
                self.Country_Name = rowa['Name']

    def checkCurrency(self):
        # Currency
        self.Currency = self.row['Currency'].strip()
        self.df_tmp = self.df_Currency[(self.df_Currency.Code == self.Currency)]
        self.rows = len(self.df_tmp.axes[0])
        self.Currency_Name = ''
        if self.rows == 0:
            self.msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Currency code'.format(self.RowNo+1,'Currency',self.Currency)
            print(self.msg)
            self.error_messages.append(self.msg)
        else:
            for _, rowa in self.df_tmp.iterrows():
                self.Currency_Name = rowa['Name']

    def checkCompany(self):
        # Company
        self.Company = self.row['Company']
        self.df_tmp = self.df_Company[(self.df_Company.Id == self.Company)]
        self.rows = len(self.df_tmp.axes[0])
        self.Company_Name = ''
        if self.rows == 0:
            self.msg = 'RowNo: {:06}: Column: {} Value: "{}" - invalid/missing Company code'.format(self.RowNo+1,'Company',self.Company)
            print(self.msg)
            self.error_messages.append(self.msg)
        else:
            for _, rowa in self.df_tmp.iterrows():
                self.Company_Name = rowa['Name']

    def getAsOfDate(self):
        # AsOfDate
        self.AsOfDate = str(datetime.datetime.now())

    def getProcessIdentifier(self):
        # ProcessIdentifier
        self.ProcessIdentifier = os.getpid()

    def writeOutput_row(self):
        # Output
        print("self.output_rowA")
        print("self.row['hash']:")
        print(self.df)
        # print(self.row['hash'])
        # print(type(self.row['hash']))
        print("self.output_rowB")

        # self.output_row = [
        #     self.RowNo+1,self.row['Deal Name'],
        #     self.row['D1'],self.row['D2'],self.row['D3'],self.row['D4'],self.row['D5'],
        #     self.row['Is Active'],self.row['Country'],self.row['Currency'],
        #     self.row['Company'],self.Company_Name,self.AsOfDate,self.ProcessIdentifier,self.row['hash']]

        self.output_row = [self.RowNo+1,self.row['Deal Name'],self.row['D1'],self.row['D2'],self.row['D3'],self.row['D4'],self.row['D5'],self.row['Is Active'],self.row['Country'],self.row['Currency'],self.row['Company'],self.Company_Name,self.AsOfDate,self.ProcessIdentifier]#,str(self.row['hash'])]

        print(self.output_row)
        self.output_writer.writerow(self.output_row)

    def writeErrorFile(self):
        for self.row in self.error_messages:
            print(self.row)
            self.error_file.write(self.row+'\n')

    def writeParquetFile(self):
        self.df = pd.read_csv(self.output_csv_file)
        self.df.to_parquet(self.output_par_file)

    def closeFiles(self):
        self.output_file.close()
        self.error_file.close()


deal = Deal()
deal.loadCsvFiles()
deal.createRowHash()
deal.loadErrParFiles()
deal.writeHeader()

# # Input CSV rows process
for deal.RowNo, deal.row in deal.df.iterrows():
    deal.checkDeal_Name()
    deal.checkDs()
    deal.checkIs_Active()
    deal.checkCountry()
    deal.checkCurrency()
    deal.checkCompany()
    deal.getAsOfDate()
    deal.getProcessIdentifier()

    deal.createRowHash()

    deal.writeOutput_row()
    # deal.writeErrorFile()
