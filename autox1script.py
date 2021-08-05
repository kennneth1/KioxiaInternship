import numpy as np 
import pandas
import glob
import os
import pandas as pd
from datetime import date
import os.path
from os import path

today = date.today()

d3 = today.strftime("%m%d")
f =r"/Users/huangk/Desktop/X1FileAuto/" + d3 + "/" + d3 + ".xls"
print("Looking in:", f, "\n")
df = pd.read_csv(f, quotechar=",")

df = df[['SSBU','PSI_GROUP', 'CUSTOMER', 'REPL_PN', 'PRODUCT_FAMILY', 
                  'RAW_CAPACITY','USABLE_CAPACITY','ENDURANCE',
                 'CQ_M1_BOH','CQ_M1_FOB','CQ_M2_FOB','CQ_M3_FOB',
                  'CQ_M4_FOB','CQ_M5_FOB','CQ_M6_FOB',
                 'CQ_M1_CUST_FCST','CQ_M2_CUST_FCST','CQ_M3_CUST_FCST',
                  'CQ_M4_CUST_FCST','CQ_M5_CUST_FCST','CQ_M6_CUST_FCST']]

print("Extracting relevant columns...")

df['RAW_CAPACITY'] = df['RAW_CAPACITY'].str.replace('GB','').astype(int)
df['USABLE_CAPACITY'] = df['USABLE_CAPACITY'].str.replace('GB','').astype(int)
df['ENDURANCE'] = df['ENDURANCE'].str.replace(' DWPD','')
df['PSI_GROUP'] = df['PSI_GROUP'].str.replace('Dell', 'DELL')
df['PSI_GROUP'] = df['PSI_GROUP'].str.replace('HPE', 'HPe')

df.loc[(df['PSI_GROUP'] == 'Channel/Other') & (df['SSBU'] == "X131")] = df.loc[(df['PSI_GROUP'] == 'Channel/Other') & (df['SSBU'] == "X131")].replace('Channel/Other', 'Generic')

print("Cleaning up formatting and renaming X131 Channel/Other to Generic...")

df.insert(12,'Q3_FOB',df['CQ_M1_FOB']+df['CQ_M2_FOB']+df['CQ_M3_FOB'])
df.insert(16,'Q4_FOB',df['CQ_M4_FOB']+df['CQ_M5_FOB']+df['CQ_M6_FOB'])
df.insert(20,'Q3_FC',df['CQ_M1_CUST_FCST']+df['CQ_M2_CUST_FCST']+df['CQ_M3_CUST_FCST'])
df.insert(24,'Q4_FC',df['CQ_M4_CUST_FCST']+df['CQ_M5_CUST_FCST']+df['CQ_M6_CUST_FCST'])
df.rename(columns={'CQ_M1_BOH':'Q3_BOH'}, inplace=True)
df.rename(columns={'CQ_M1_FOB':'JULY_FOB'}, inplace=True)
df.rename(columns={'CQ_M2_FOB':'AUG_FOB'}, inplace=True)
df.rename(columns={'CQ_M3_FOB':'SEP_FOB'}, inplace=True)
df.rename(columns={'CQ_M4_FOB':'OCT_FOB'}, inplace=True)
df.rename(columns={'CQ_M5_FOB':'NOV_FOB'}, inplace=True)
df.rename(columns={'CQ_M6_FOB':'DEC_FOB'}, inplace=True)

df.rename(columns={'CQ_M1_CUST_FCST':'JULY_FC'}, inplace=True)
df.rename(columns={'CQ_M2_CUST_FCST':'AUG_FC'}, inplace=True)
df.rename(columns={'CQ_M3_CUST_FCST':'SEP_FC'}, inplace=True)
df.rename(columns={'CQ_M4_CUST_FCST':'OCT_FC'}, inplace=True)
df.rename(columns={'CQ_M5_CUST_FCST':'NOV_FC'}, inplace=True)
df.rename(columns={'CQ_M6_CUST_FCST':'DEC_FC'}, inplace=True)

PN_10 =df['REPL_PN'].str[:-2] #strip last 2 chars from REPL PN
df.insert(4,'10_dig_PN',PN_10)

print("Generating column totals and more renaming...")

key_cols = ['Q3_BOH','Q3_FOB','Q4_FOB','Q3_FC','Q4_FC']
for i in key_cols:
    if df[i].sum()==0:
        print("\n")
        print("WARNING:", i ," contains no data")
        print("\n")

null_rows = []
for index, row in df.iterrows():
    if "-" in row['REPL_PN'] and row['Q3_BOH'] == 0 and row['Q3_FOB'] == 0 and row['Q3_FC'] == 0 and row['Q4_FC'] == 0:
        null_rows.append(index)

print("\n")
print("Counted and removed", len(null_rows), " null rows with overly lengthy part numbers...")
rows = df.index[null_rows]
df.drop(rows, inplace=True)

### Merging exact PN's on full DF ###
aggregation_functions = {'Q3_BOH': 'sum', 'JULY_FOB': 'sum', 'AUG_FOB': 'sum','SEP_FOB': 'sum','Q3_FOB': 'sum',
                        'OCT_FOB': 'sum','NOV_FOB': 'sum','DEC_FOB': 'sum','Q4_FOB': 'sum',
                        'JULY_FC': 'sum', 'AUG_FC': 'sum','SEP_FC': 'sum','Q3_FC': 'sum','OCT_FC': 'sum',
                        'NOV_FC': 'sum','DEC_FC': 'sum','Q4_FC': 'sum'}

merged_df = df.groupby(['SSBU','PSI_GROUP','CUSTOMER','REPL_PN','10_dig_PN', 'PRODUCT_FAMILY','RAW_CAPACITY','USABLE_CAPACITY',
                 'ENDURANCE']).agg(aggregation_functions).reset_index()

print((len(df)-len(merged_df)), "rows lost due to full 10 digit PN df merge...")

### Merging on first CUSTOMER, and exact PN's ###
X131 = df.loc[(df['SSBU'] =="X131")]
X111_DELL_HPI = df.loc[(df['SSBU'] =="X111") & ((df['PSI_GROUP'] =="DELL") | (df['PSI_GROUP'] =="HPI"))]
Others = df.loc[(df['SSBU'] == "X121") | ((df['SSBU'] == "X111") & ((df['PSI_GROUP'] != "DELL") & (df['PSI_GROUP'] != "HPI")))]

agg_functions = {'Q3_BOH': 'sum', 'JULY_FOB': 'sum', 'AUG_FOB': 'sum','SEP_FOB': 'sum','Q3_FOB': 'sum',
                        'OCT_FOB': 'sum','NOV_FOB': 'sum','DEC_FOB': 'sum','Q4_FOB': 'sum',
                        'JULY_FC': 'sum', 'AUG_FC': 'sum','SEP_FC': 'sum','Q3_FC': 'sum','OCT_FC': 'sum',
                        'NOV_FC': 'sum','DEC_FC': 'sum','Q4_FC': 'sum', 'CUSTOMER':'first'}

merged_X111_DELL_HPI = X111_DELL_HPI.groupby(['SSBU','PSI_GROUP','REPL_PN','10_dig_PN', 'PRODUCT_FAMILY','RAW_CAPACITY','USABLE_CAPACITY',
                 'ENDURANCE']).agg(agg_functions).reset_index()

print((len(X111_DELL_HPI)-len(merged_X111_DELL_HPI)), "rows lost due to BU X111 Dell + HPI merge...")

merged_X131 = X131.groupby(['SSBU','PSI_GROUP','REPL_PN','10_dig_PN', 'PRODUCT_FAMILY','RAW_CAPACITY','USABLE_CAPACITY',
                 'ENDURANCE']).agg(agg_functions).reset_index()

print((len(X131)-len(merged_X131)), "rows lost due to BU X131 merge...")

columnsTitles = ['SSBU','PSI_GROUP','CUSTOMER','REPL_PN','10_dig_PN','PRODUCT_FAMILY','RAW_CAPACITY',
                           'USABLE_CAPACITY','ENDURANCE','Q3_BOH','JULY_FOB','AUG_FOB','SEP_FOB','Q3_FOB',
                           'OCT_FOB','NOV_FOB','DEC_FOB','Q4_FOB','JULY_FC','AUG_FC','SEP_FC','Q3_FC','OCT_FC',
                           'NOV_FC','DEC_FC','Q4_FC']

merged_X131 = merged_X131.reindex(columns=columnsTitles)
merged_X111_DELL_HPI = merged_X111_DELL_HPI.reindex(columns=columnsTitles)
df = pd.concat([merged_X131, merged_X111_DELL_HPI, Others])
df['Q3_KIC_P'] = ""
df['Q4_KIC_P'] = ""
df.insert(0,'X131LOOKUP',df['PSI_GROUP']+df['REPL_PN'])
df.insert(0,'LOOKUPFAM',df['PSI_GROUP']+df['REPL_PN']+df['PRODUCT_FAMILY'])
df.insert(0,'LOOKUPMART',df['PSI_GROUP']+df['CUSTOMER']+df['REPL_PN']+df['PRODUCT_FAMILY'])
print("Inserting useful VLOOKUP columns...")
print("\n")
print(df.head(10))

 
today = date.today()
d3 = today.strftime("%m%d")
filename = "/Users/huangk/Desktop/X1FileAuto/" + d3  + "/" + d3 + "X1CLEANED.xlsx"
if path.exists(filename):
    print("The cleaned data already exists, code will prompt user for next steps")
else:
    df.to_excel(filename,sheet_name='main')
    print("\n")
    print("Data is of size: ", df.shape, ". File: ", filename, " is now ready for KIC Lookups")

################################################################################
print("Did you: \n")
print("- Manually adjust KIC, Demand, FC for X131 HPe RM5 RM6?")
print("- Add AWS from Martin's file?")
print("- Check overall KIC P accuracy?")
print("- Set 740 as X111 Hpe KXG6AZNV1T02 KIC Q4?")
print("- Set Q3 X111 Hpe KIC = Q3 FOB total?")
print("- Ready to run script on KIC-filled file?")
s = ''
while s!="y":
    s = input("Enter y if file is KIC-filled, properly named, stored, and ready to finalize: ")

path2 = "/Users/huangk/Desktop/X1FileAuto/" + d3 + "/" + d3 + "X1CLEANEDKIC.xlsx"
full = pd.read_excel(io=path2, sheet_name='main')
full = full.drop(columns=['LOOKUPMART', 'LOOKUPFAM', 'X131LOOKUP'])
full['PRODUCT_FAMILY'] = full['PRODUCT_FAMILY'].str.replace('BG4a','BG4')

X131 = full.loc[(full['SSBU'] =="X131")]
X111_CO = full.loc[(full['SSBU'] =="X111") & (full['PSI_GROUP'] == "Channel/Other") ]
X111_DELL_HPI = full.loc[(full['SSBU'] =="X111") & ((full['PSI_GROUP'] == "DELL") | (full['PSI_GROUP'] == "HPI"))]
X111_reg = full.loc[((full['SSBU'] =="X111") & ((full['PSI_GROUP'] != "Channel/Other") 
                    & (full['PSI_GROUP'] != "DELL") & (full['PSI_GROUP'] != "HPI")))]
X121 = full.loc[(full['SSBU'] =="X121")]
print("\n")
print("Splitting dataset into respective SSBU's...")
##########################################################################

X131 = X131.sort_values(by=['Q3_KIC_P'], ascending = False)
aggregation_functions = {'Q3_BOH': 'sum', 'JULY_FOB': 'sum', 'AUG_FOB': 'sum','SEP_FOB': 'sum','Q3_FOB': 'sum',
                        'OCT_FOB': 'sum','NOV_FOB': 'sum','DEC_FOB': 'sum','Q4_FOB': 'sum',
                        'JULY_FC': 'sum', 'AUG_FC': 'sum','SEP_FC': 'sum','Q3_FC': 'sum','OCT_FC': 'sum',
                        'NOV_FC': 'sum','DEC_FC': 'sum','Q4_FC': 'sum', 'CUSTOMER':'first', 'REPL_PN': 'first', 
                         'Q3_KIC_P':'first','Q4_KIC_P':'first'}

merged_X131 = X131.groupby(['SSBU','PSI_GROUP','10_dig_PN', 'PRODUCT_FAMILY','RAW_CAPACITY','USABLE_CAPACITY',
                 'ENDURANCE']).agg(aggregation_functions).reset_index()

print((len(X131)-len(merged_X131)), "rows lost due to X131 merge...")
print(""" Verifying X131 Merge Results""")
print(X131['Q3_BOH'].sum(),merged_X131['Q3_BOH'].sum())
print(X131['Q4_FC'].sum(),merged_X131['Q4_FC'].sum())
print(X131['Q3_KIC_P'].sum(),merged_X131['Q3_KIC_P'].sum())
print(X131['Q4_KIC_P'].sum(),merged_X131['Q4_KIC_P'].sum())
print("\n")



X111_CO = X111_CO.sort_values(by=['Q3_KIC_P'], ascending = False)
merged_X111_CO = X111_CO.groupby(['SSBU','PSI_GROUP','10_dig_PN', 'PRODUCT_FAMILY','RAW_CAPACITY','USABLE_CAPACITY',
                 'ENDURANCE']).agg(aggregation_functions).reset_index()

print((len(X111_CO)-len(merged_X111_CO)), "rows lost due to X111 Generic merge...")
print(""" Verifying X111 Channel/Other Merge Results""")
print(X111_CO['Q3_FC'].sum(),merged_X111_CO['Q3_FC'].sum())
print(X111_CO['Q4_FC'].sum(),merged_X111_CO['Q4_FC'].sum())
print(X111_CO['Q3_KIC_P'].sum(),merged_X111_CO['Q3_KIC_P'].sum())
print(X111_CO['Q4_KIC_P'].sum(),merged_X111_CO['Q4_KIC_P'].sum())
print("\n")


X111_DELL_HPI = X111_DELL_HPI.sort_values(by=['Q3_KIC_P'], ascending = False)
aggr_functions = {'Q3_BOH': 'sum', 'JULY_FOB': 'sum', 'AUG_FOB': 'sum','SEP_FOB': 'sum','Q3_FOB': 'sum',
                        'OCT_FOB': 'sum','NOV_FOB': 'sum','DEC_FOB': 'sum','Q4_FOB': 'sum',
                        'JULY_FC': 'sum', 'AUG_FC': 'sum','SEP_FC': 'sum','Q3_FC': 'sum','OCT_FC': 'sum',
                        'NOV_FC': 'sum','DEC_FC': 'sum','Q4_FC': 'sum', 'Q3_KIC_P': 'sum','Q4_KIC_P': 'sum'}
merged_X111_DELL_HPI = X111_DELL_HPI.groupby(['SSBU','PSI_GROUP','CUSTOMER','REPL_PN','10_dig_PN'
                            , 'PRODUCT_FAMILY','RAW_CAPACITY','USABLE_CAPACITY',
                 'ENDURANCE']).agg(aggr_functions).reset_index()
print((len(X111_DELL_HPI)-len(merged_X111_DELL_HPI)), "rows lost due to X111 Dell HPI merge...")
print(""" Verifying X111_DELL_HPI Merge Results""")
print(X111_DELL_HPI['Q3_FC'].sum(),merged_X111_DELL_HPI['Q3_FC'].sum())
print(X111_DELL_HPI['Q4_FC'].sum(),merged_X111_DELL_HPI['Q4_FC'].sum())
print(X111_DELL_HPI['Q3_KIC_P'].sum(),merged_X111_DELL_HPI['Q3_KIC_P'].sum())
print(X111_DELL_HPI['Q4_KIC_P'].sum(),merged_X111_DELL_HPI['Q4_KIC_P'].sum())
print("\n")

columnsTitles = ['SSBU','PSI_GROUP','CUSTOMER','REPL_PN','10_dig_PN','PRODUCT_FAMILY','RAW_CAPACITY',
                           'USABLE_CAPACITY','ENDURANCE','Q3_BOH','JULY_FOB','AUG_FOB','SEP_FOB','Q3_FOB',
                           'OCT_FOB','NOV_FOB','DEC_FOB','Q4_FOB','JULY_FC','AUG_FC','SEP_FC','Q3_FC','OCT_FC',
                           'NOV_FC','DEC_FC','Q4_FC' , 'Q3_KIC_P', 'Q4_KIC_P']

merged_X111_CO = merged_X111_CO.reindex(columns=columnsTitles)
merged_X131 = merged_X131.reindex(columns=columnsTitles)
merged_X111_DELL_HPI = merged_X111_DELL_HPI.reindex(columns=columnsTitles)

df_full = pd.concat([merged_X131, merged_X111_CO, X121, X111_reg, merged_X111_DELL_HPI])
############################################################################

yy = df_full.shape
print("\nData has been recombined, and is of shape",yy, "...")
df_full = df_full.reset_index()
df_full.index

Q4_KIC_FOB_compare = [[df_full.loc[(df_full['SSBU'] =="X131")]['Q4_KIC_P'].sum(), df_full.loc[(df_full['SSBU'] =="X131")]['Q4_FOB'].sum()]
                      ,[df_full.loc[(df_full['SSBU'] =="X121")]['Q4_KIC_P'].sum(),df_full.loc[(df_full['SSBU'] =="X121")]['Q4_FOB'].sum()]
                      ,[df_full.loc[(df_full['SSBU'] =="X111")]['Q4_KIC_P'].sum(),df_full.loc[(df_full['SSBU'] =="X111")]['Q4_FOB'].sum()]]

# If KIC is severely lacking (less than 10% of FOB)
li = ["X131","X121","X111"]
i=0
for pair in Q4_KIC_FOB_compare:
    if (pair[1]/10)>pair[0]:
        print("please remember to use Q4 FOB in place of KIC P for", li[i])
        i+=1
    i+=1

indexNames = df_full[(df_full['Q3_BOH'] == 0) & (df_full['Q3_FOB'] == 0) & (df_full['Q4_FOB']) & (df_full['Q4_FC'] == 0) & (df_full['Q3_KIC_P'] == 0) & (df_full['Q4_KIC_P'] == 0) ].index
print("Counted and dropped:", len(indexNames), "fully null rows")
df_full.drop(indexNames , inplace=True, axis=0)
df_full.shape

### Formula Generation ###
Q3_EOH = np.where(df_full['Q3_BOH']+df_full['Q3_FOB']-df_full['Q3_FC'] < 0, 
                  0, df_full['Q3_BOH']+df_full['Q3_FOB']-df_full['Q3_FC'])

df_full['Q3_EOH'] = Q3_EOH


Q4_EOH = np.where(df_full['Q3_EOH']+df_full['Q4_KIC_P']-df_full['Q4_FC'] < 0, 
                  0, df_full['Q3_EOH']+df_full['Q4_KIC_P']-df_full['Q4_FC'])
df_full['Q4_EOH'] = Q4_EOH
df_full = df_full.drop(columns=['index'])

df_full['Q3_ALIGNED_SUPP'] = '' 
df_full['Q3_SUP'] = df_full['Q3_FOB'] + df_full['Q3_BOH']
df_full['Q3_ALIGNED_SUPP'] = df_full[['Q3_SUP', 'Q3_FC']].min(axis = 1)
df_full['Q3_REV_BASE'] =  df_full['Q3_ALIGNED_SUPP']-df_full['Q3_ALIGNED_SUPP']/13

df_full['Q4_ALIGNED_SUPP'] = '' 
df_full['Q4_SUP'] = df_full['Q4_KIC_P'] + df_full['Q3_EOH']
df_full['Q4_ALIGNED_SUPP'] = df_full[['Q4_SUP', 'Q4_FC']].min(axis = 1)
df_full['Q4_REV_BASE'] = df_full['Q4_ALIGNED_SUPP']-df_full['Q4_ALIGNED_SUPP']/13

df_full['Q3_RISK'] = df_full['Q3_ALIGNED_SUPP']/13
df_full['Q4_RISK'] = df_full['Q4_ALIGNED_SUPP']/13

df_full['Q3_BOTTOM'] = df_full['Q3_ALIGNED_SUPP']-2*df_full['Q3_RISK']
df_full['Q4_BOTTOM'] = df_full['Q4_ALIGNED_SUPP']-2*df_full['Q4_RISK']

df_full['Q3_BEST'] = df_full[['Q3_SUP', 'Q3_FC']].max(axis = 1)
df_full['Q4_BEST'] = df_full[['Q4_SUP', 'Q4_FC']].max(axis = 1)

df_full['Q3_DEMAND_OPP'] = df_full['Q3_FC'] - df_full['Q3_REV_BASE']
df_full['Q4_DEMAND_OPP'] = df_full['Q4_FC'] - df_full['Q4_REV_BASE']

df_full['Q3_SUPP_OPP'] = df_full['Q3_SUP'] - df_full['Q3_ALIGNED_SUPP']
df_full['Q4_SUPP_OPP'] = df_full['Q4_SUP'] - df_full['Q4_ALIGNED_SUPP']

df_full['Q3_SUPP'] = df_full['Q3_SUPP_OPP'] + df_full['Q3_DEMAND_OPP'] # totals
df_full['Q4_SUPP'] = df_full['Q4_SUPP_OPP'] + df_full['Q4_DEMAND_OPP']

df_full = df_full.drop(columns=['Q3_SUP', 'Q4_SUP'])

print("Added extra PB/uPB columns")

cols = ['Q3_BOH','JULY_FOB','AUG_FOB','SEP_FOB','Q3_FOB','OCT_FOB','NOV_FOB','DEC_FOB','Q4_FOB','JULY_FC','AUG_FC','SEP_FC',
        'Q3_FC','OCT_FC','NOV_FC','DEC_FC','Q4_FC','Q3_KIC_P','Q4_KIC_P','Q3_EOH','Q4_EOH','Q3_ALIGNED_SUPP','Q3_REV_BASE',
        'Q4_ALIGNED_SUPP','Q4_REV_BASE','Q3_RISK','Q4_RISK','Q3_BOTTOM','Q4_BOTTOM','Q3_BEST','Q4_BEST',
       'Q3_DEMAND_OPP','Q4_DEMAND_OPP','Q3_SUPP_OPP','Q4_SUPP_OPP','Q3_SUPP','Q4_SUPP']

for col in cols:
    title = col+"_PB"
    data = (df_full[col]*df_full['RAW_CAPACITY'])/1000000
    df_full[title]=data
    
for col in cols:
    title = col+"_uPB"
    data = (df_full[col]*df_full['USABLE_CAPACITY'])/1000000
    df_full[title]=data
df_full = df_full.drop(columns=['10_dig_PN'])
# Grouping together X131 Dell/EMC    
df_full.loc[(df_full['PSI_GROUP'] == 'EMC') & (df_full['SSBU'] == "X131")] = df_full.loc[(df_full['PSI_GROUP'] == 'EMC') & (df_full['SSBU'] == "X131")].replace('EMC', 'DELL')
print("Full dataset completed...\n")

today = date.today()
d = today.strftime("%m%d")
filename = "/Users/huangk/Desktop/X1FileAuto/" + d3 + "/" + d3 + "X1FILE.xlsx"
df_full.to_excel(filename,sheet_name='main')  
print(filename, "saved to desktop/local drive...")
print("Remember to check if FOB or KIC is better supply data for Q4 formulas")
