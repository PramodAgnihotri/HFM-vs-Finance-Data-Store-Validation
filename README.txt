
import pandas as pd
import numpy as np
import smtplib
# from email.mine.text import MIMEText
# import time
# import csv


pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 20)

# Please set the path here
path = ""

fds_File = path+"/FDS1.txt"
hfm_File = path+"/HFM1.txt"

# Reading FDS file with appropriate rows
try:
    FDS = fds_File
    FDS_Col = ['Col1','Col2','Col3','Col4','Col5','Col6','Col7','Col8','Rev Type','Col9','Col10','Col11','Col12','Col13','Col14','Col15']   # You can Input your own Headers here.
    Unused_rows_FDS = ['Col1','Col2','Col5','Col6','Col7','Col8','Rev Type']   #The Rows which are not to be considered
    FDS_ip_error = path+"/FDS_Col8Error.txt"
except:
    print("Error in reading FDS input file")


# Reading HFM file with appropriate rows
try:
    HFM = hfm_File
    HFM_Col = ['Col10','Col9','Mon','Col11','Col4','Col14','Col12','Col5','LOB','TopC2','TopC3','TopC4','Col15']
    Unused_rows_HFM = ['TopC2','TopC3','TopC4','Col5']
    HFM_ip_error = path+"/HFM_Col8Error.txt"
except:
    print("Error in reading HFM Input files")


# Read all CSV into a dataframe and assign accordingly to FDS and HFM by considering and dropping necessary columns
df1 = pd.read_csv(FDS, error_bad_lines=False ,warn_bad_lines=True ,delimiter=';', names=FDS_Col, decimal=',',dtype={'Col3': object , 'Col12': object})
FDS = df1.drop(Unused_rows_FDS, axis = 1)
df2 = pd.read_csv(HFM,error_bad_lines=False,warn_bad_lines=True ,delimiter=';', names=HFM_Col,dtype={'LOB': object , 'Col12': object})
HFM = df2.drop(Unused_rows_HFM, axis=1)

# Setting the delimiter
delim = ';'


#   Cleaning FDS Datafile


# Chopping 'C' from Col3anization
FDS['Col3'] = FDS['Col3'].astype(str).str.slice(3)
# Converting Col14 into 'USD'
FDS['Col14'] = FDS['Col14'].astype(str).str.slice(0,3,1)
# Taking just 2 digits of the year
FDS['Col9'] = FDS['Col9'].astype(str).str.slice(2)

# Cleaning Col13
FDS['1'] = np.where((FDS['Col13'] == 'Jan') | (FDS['Col13'] == 'Feb') | (FDS['Col13'] == 'Mar'),"Q1",'')
FDS['2'] = np.where((FDS['Col13'] == 'Apr') | (FDS['Col13'] == 'May') | (FDS['Col13'] == 'Jun'),"Q2",'')
FDS['3'] = np.where((FDS['Col13'] == 'Sep') | (FDS['Col13'] == 'Aug') | (FDS['Col13'] == 'Jul'),"Q3",'')
FDS['4'] = np.where((FDS['Col13'] == 'Oct') | (FDS['Col13'] == 'Nov') | (FDS['Col13'] == 'Dec'),"Q4",'')
FDS['Col13'] = FDS['1']+FDS['2']+FDS['3']+FDS['4']
FDS['Col13'] = FDS['Col13'].str.strip()
FDS = FDS.drop(['1','2','3','4'],axis = 1)


# Converts the column values into float and rounds to 2 digit
FDS['Col15'] = FDS['Col15'].replace(',','',regex=True).astype(float)
FDS['Col15'] = FDS['Col15'].round(1)
pd.to_numeric(FDS['Col15'])
FDS.drop(FDS[FDS['Col15'] == 0].index, inplace=True)

# Function to trim the entity to sync it with HFM.
def Col4_FDS_Fix():
    FDS['Col4'] = FDS['Col4'].astype(str).str.slice(0)
    FDS['Col4'] = np.where(FDS['Col4'].str.match('E[\d\w_]'),  FDS.Col4.str.slice(1) , FDS['Col4'])
    FDS.drop(FDS[FDS['Col4'].str.match('\d{6}[A-Z]{3}')].index, inplace = True)
    FDS.drop(FDS[FDS['Col4'].str.match('Corp_Elim')].index, inplace=True)
    FDS.drop(FDS[FDS['Col4'].str.match('CORP_ELIM')].index, inplace=True)
Col4_FDS_Fix()

# Aligning the dimensions' order
FDS = FDS[['Col10','Col9','Col11','Col4','Col14','Col12','Col3','Col13','Col15']]
# Assigning all dimensions to a single column
FDS['Dim'] = FDS['Col10'].map(str)+delim+FDS['Col9'].map(str)+delim+FDS['Col4'].map(str)+delim+FDS['Col14'].map(str)+delim+FDS['Col12'].map(str)+delim+FDS['Col3'].map(str)+delim+FDS['Col13'].map(str)+delim+FDS['Col15'].map(str)
FDS['Dim'] = FDS['Dim'].str.replace(' ','')
# Delete rest of the columns and just have dim
FDS = FDS.drop(['Col10','Col9','Col11','Col4','Col14','Col12','Col3','Col13','Col15'],axis = 1)
# Convert this file to CSV
FDS.to_csv (path+"/FDSCleaned.txt", sep=";",index = False,header=None)

#   Cleaning HFM Datafile

# Trimming the entity accordingly
def Col4_Fix():
    # Convert the field to string
    HFM['Col4'] = HFM['Col4'].astype(str).str.slice(0)
    # If 6Digits followed by letter, consider only the digite or else consider the whole field
    HFM['Col4'] = np.where(HFM['Col4'].str.match('\d{6}\w_\w+'), HFM.Col4.str.slice(0, 6, 1), HFM['Col4'])
    HFM['Col4'] = np.where(HFM['Col4'].str.match('\d{6}\w'), HFM.Col4.str.slice(0,-1,1), HFM['Col4'])
Col4_Fix()

#Fixing LOB
def fixLOB():
    HFM['LOB'] = np.where(HFM['LOB'].str.match('\w\d{5}'), HFM.LOB.str.slice(3), HFM['LOB'])
fixLOB()

# Chop the digits only of the year
HFM['Col9'] = HFM['Col9'].astype(str).str.slice(2)
# Consider only USD part of currency
HFM['Col14'] = HFM['Col14'].astype(str).str.slice(0,3,1)
# Convert amount to float and round it
HFM['Col15'] = HFM['Col15'].replace(',','',regex=True).astype(float)
HFM['Col15'] = HFM['Col15'].round(1)

HFM.drop(HFM[HFM['Col15'] == 0].index, inplace=True)

# Convert month o string
HFM['Mon'] = HFM['Mon'].astype(str)
# Converting the Months in HFM to Qtr as a new dimension
HFM['1'] = np.where((HFM['Mon'] == 'Jan') | (HFM['Mon'] == 'Feb') | (HFM['Mon'] == 'Mar'),"Q1",'')
HFM['2'] = np.where((HFM['Mon'] == 'Apr') | (HFM['Mon'] == 'May') | (HFM['Mon'] == 'Jun'),"Q2",'')
HFM['3'] = np.where((HFM['Mon'] == 'Sep') | (HFM['Mon'] == 'Aug') | (HFM['Mon'] == 'Jul'),"Q3",'')
HFM['4'] = np.where((HFM['Mon'] == 'Oct') | (HFM['Mon'] == 'Nov') | (HFM['Mon'] == 'Dec'),"Q4",'')
HFM['Qtr'] = HFM['1']+HFM['2']+HFM['3']+HFM['4']
HFM['Qtr'] = HFM['Qtr'].str.strip()
HFM = HFM.drop(['1','2','3','4','Mon'],axis = 1)
# Aligning the dimensions
HFM = HFM[['Col10','Col9','Col11','Col4','Col14','Col12','LOB','Qtr','Col15']]
HFM['Dim'] = HFM['Col10'].map(str)+delim+HFM['Col9'].map(str)+delim+HFM['Col4'].map(str)+delim+HFM['Col14'].map(str)+delim+HFM['Col12'].map(str)+delim+HFM['LOB'].map(str)+delim+HFM['Qtr'].map(str)+delim+HFM['Col15'].map(str)
HFM = HFM.drop(['Col10','Col9','Col11','Col4','Col14','Col12','LOB','Qtr','Col15'],axis = 1)
# Writing into a CSV
HFM.to_csv (path+"/HFMCleaned.txt", sep=";",index = False,header=None)

# Validating the prepared dataset

def Validate():


    HFM_Out = pd.concat([FDS,HFM, HFM]).drop_duplicates(keep = False) # Backup, HFM output
    print(HFM_Out)
    FDS_Out = pd.concat([HFM,FDS, FDS]).drop_duplicates(keep = False) # Backup, FDS output
    print(FDS_Out)

    HFM_Out.to_csv(path+"/MissedFromHFM.txt", sep=";", index=False, header=None)
    FDS_Out.to_csv (path+"/MissedInFDS.txt", sep=";",index = False,header=None)

    HFM_Good = pd.merge(FDS, HFM, how='inner') # Merge both files with common items and get the only items which are non mutual
    HFM_Good.to_csv(path+"/GoodRecords.txt", sep=";", index=False, header=None)

Validate()
