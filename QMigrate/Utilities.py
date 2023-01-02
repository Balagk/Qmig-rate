import sys

import pandas as pd
from numpy import unique
from sqlalchemy import create_engine
import pandasql as ps
pd.options.mode.chained_assignment = None

SOURCE_DUMP_PATH="Dumps\\SOURCE"
TARGET_DUMP_PATH="Dumps\\TARGET"

def read_dump_file(DumpStage,DumpName):
    df=pd.DataFrame()
    if (DumpStage.upper() == 'SOURCE'):
        df = pd.read_csv("Dumps\\SOURCE\\" + DumpName + ".csv", dtype=str)
    elif (DumpStage.upper() == 'TARGET'):
        df = pd.read_csv("Dumps\\TARGET\\" + DumpName + ".csv", dtype=str)
    #     print (pd.options.display.max_columns)
    #     pd.options.display.max_columns = 500
    return df

def read_database_table(DumpStage,tableName,sql):
    df=pd.DataFrame()
    DBConnectionDetailsDF = pd.read_excel("Driver.xlsx", sheet_name="DB_Connections")
    if(DumpStage.upper()=='SOURCE'):
        DBConnectionDetailsDF = DBConnectionDetailsDF[DBConnectionDetailsDF['RESOURCE_TYPE'] == 'SOURCE']
    elif(DumpStage.upper()=='TARGET'):
        DBConnectionDetailsDF = DBConnectionDetailsDF[DBConnectionDetailsDF['RESOURCE_TYPE'] == 'TARGET']
    DATABASE_TYPE=""
    HOST=""
    PORT=""
    DB_NAME=""
    USERNAME=""
    PASSWORD=""
    DB_LibraryName=""
    for index, row in DBConnectionDetailsDF.iterrows():
        DATABASE_TYPE = str(row['DATABASE_TYPE'])
        HOST = str(row['HOST'])
        PORT = str(row['PORT'])
        DB_NAME = str(row['DB/SERVICE_NAME'])
        USERNAME = str(row['USERNAME'])
        PASSWORD = str(row['PASSWORD'])
    if(DATABASE_TYPE=="ORACLE"):
        DB_LibraryName='oracle+cx_oracle'
    elif(DATABASE_TYPE=="PostgerSQL"):
        DB_LibraryName = 'postgresql+psycopg2'
    elif(DATABASE_TYPE=="MYSQL"):
        DB_LibraryName = 'mysql+mysqlconnector'

    connection_string = DB_LibraryName+'://'+USERNAME+':'+PASSWORD+'@'+HOST+':'+PORT+'/'+DB_NAME
    alchemyEngine = create_engine(connection_string)
    dbConnection = alchemyEngine.connect()
    if(sql=="NULL"):
        df = pd.read_sql_table(tableName, dbConnection)
    else:
        df = pd.read_sql(sql, dbConnection)

    # return df.applymap(str)
    return df

def convert(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    return "%d:%02d:%02d" % (hour, minutes, seconds)

def splitColumns(df,sourceColumn,TargetColumns):
    TargetColumnsListMain = TargetColumns.split("(")
    TargetColumnsList=TargetColumnsListMain[0].split(",")
    seperater = TargetColumnsListMain[1].replace(")", "")
    if(seperater!=""):
        df[TargetColumnsList] = df[sourceColumn].str.split(seperater, expand=True)
    else:
        # n=1
        df[TargetColumnsList] = df[sourceColumn].str.split(' ',1, expand=True)

    columns = [sourceColumn]
    df.drop(columns, inplace=True, axis=1)
    return df

def ReplaceMapColumnFromDepTable(df,sourceColumn,TargetColumns):
    leftdf=df
    joinOn = sourceColumn
    rightdf_resource=(TargetColumns.split(","))[0]
    rightdf_name=(TargetColumns.split(","))[1]
    Req_columnsFromRight_List = ((((TargetColumns.split(","))[2]).replace("[","").replace("]",""))+","+joinOn).split(",")
    rightdf=None
    if(rightdf_resource=='source'):
        rightdf=read_dump_file('SOURCE',rightdf_name)
    elif (rightdf_resource == 'target'):
        rightdf=read_dump_file('TARGET', rightdf_name)

    rightdf = rightdf[Req_columnsFromRight_List]
    leftdf = leftdf.merge(rightdf, on=joinOn, how='left')
    leftdf.drop([joinOn], inplace=True, axis=1)
    return leftdf

def dateformater(df,sourcecolumn,TargetFormat):
    TargetFormatList = TargetFormat.split("(")
    TargetColumn=TargetFormatList[0]
    TargetFormater=TargetFormatList[1].replace(")","")
    df[sourcecolumn] = pd.to_datetime(df[sourcecolumn])
    df[sourcecolumn] = df[sourcecolumn].dt.strftime(TargetFormater)
    #'%m-%d-%Y'
    #'%d-%m-%Y'
    #'%B %d, %Y'
    df[sourcecolumn] = df[sourcecolumn].astype(str)
    if(sourcecolumn!=TargetColumn):
        df=df.rename(columns={sourcecolumn:TargetColumn})
    return df

def mergeNumericColumns_sum(df,sourceColumns,TargetColumns):
    sourceColumnsList=sourceColumns.split(",")
    TargetColumnsList=TargetColumns.split(",")
    # seperater=TargetColumnsList[1].replace(")","")
    df[sourceColumnsList] = df[sourceColumnsList].apply(pd.to_numeric)
    df[TargetColumnsList[0]] = df[sourceColumnsList[0]] + df[sourceColumnsList[1]]
    columns = sourceColumnsList
    df.drop(columns, inplace=True, axis=1)
    return df.applymap(str)

def mergeColumns(df,sourceColumns,TargetColumns):
    sourceColumnsList=sourceColumns.split(",")
    TargetColumnsList=TargetColumns.split("(")
    seperater=TargetColumnsList[1].replace(")","")
    if(seperater !=""):
        df[TargetColumnsList[0]] = df[sourceColumnsList[0]] + seperater +  df[sourceColumnsList[1]].astype(str)
    else:
        df[TargetColumnsList[0]] = df[sourceColumnsList[0]] + "" + df[sourceColumnsList[1]].astype(str)

    columns = sourceColumnsList
    df.drop(columns, inplace=True, axis=1)
    return df

def country_to_codes(df,sourceColumn,TargetColumn):
    CountryCodesDF=pd.read_excel("DUMPS\\TARGET\\COUNTRY_CODES.xlsx",sheet_name="COUNTRY_CODES")
    codes_dict = dict(zip(CountryCodesDF.COUNTRY_NAME, CountryCodesDF.COUNTRY_CODE))
    df[sourceColumn] = df[sourceColumn].map(codes_dict)
    if (sourceColumn != TargetColumn):
        df = df.rename(columns={sourceColumn: TargetColumn})
    return df

def dataframe_difference(df1,df2,which=None):
    diff_df=pd.DataFrame()
    comparision_df=df1.merge(df2,indicator=True,how='outer')
    if which is None:
        diff_df=comparision_df[comparision_df['_merge']!='both']
    else:
        diff_df=comparision_df[comparision_df['_merge']==which]
    return diff_df

def dataframe_difference1(df1,df2,primaryKeyList,which=None):
    diff_df=pd.DataFrame()
    # comparision_df=df1.merge(df2,indicator=True,how='left',on=primaryKeyList)
    comparision_df=pd.merge(df1, df2,indicator=True,how='outer',on=primaryKeyList,suffixes=('_SOURCE', '_TARGET'))
    if which is None:
        diff_df=comparision_df[comparision_df['_merge']!='both']
    else:
        diff_df=comparision_df[comparision_df['_merge']==which]
    return diff_df

def html_syntax(OverviewDF_Matching_Count,OverviewDF_Not_Matching_Count,html):
    start_html = """<!DOCTYPE html>
            <html>
            <head>
            <style>
            #customers {
              font-family: Arial, Helvetica, sans-serif;
              border-collapse: collapse;
              width: 100%;
            }
            #customers td, #customers th {
              border: 1px solid #ddd;
              padding: 8px;
            }
            #customers tr:nth-child(even){background-color: #f2f2f2;}

            #customers tr:hover {background-color: #ddd;}

            #customers th {
              padding-top: 12px;
              padding-bottom: 12px;
              text-align: left;
              background-color: #04AA6D;
              color: white;
            }
            </style>
            </head>
            <body>
            """
    headerhtml = """<h1 style="text-align:center;background-color:powderblue;">Overview Report</h1>""" \
                 """<p>Total Tables-""" + str(OverviewDF_Matching_Count+OverviewDF_Not_Matching_Count) + "</p>" \
                                                               """<p style="color:green;">Matching-""" + str(
        OverviewDF_Matching_Count) + "</p>" \
                                     """<p style="color:red;">Not Matching-""" + str(
        OverviewDF_Not_Matching_Count) + "</p>"
    finalhtml = start_html + headerhtml + html
    return finalhtml

def obj_size_fmt(num):
    if num<10**3:
        return "{:.2f}{}".format(num,"B")
    elif ((num>=10**3)&(num<10**6)):
        return "{:.2f}{}".format(num/(1.024*10**3),"KB")
    elif ((num>=10**6)&(num<10**9)):
        return "{:.2f}{}".format(num/(1.024*10**6),"MB")
    else:
        return "{:.2f}{}".format(num/(1.024*10**9),"GB")


def memory_usage():
    memory_usage_by_variable=pd.DataFrame({k:sys.getsizeof(v) for (k,v) in globals().items()},index=['Size'])

    memory_usage_by_variable=memory_usage_by_variable.T

    memory_usage_by_variable=memory_usage_by_variable.sort_values(by='Size',ascending=False).head(10)

    memory_usage_by_variable['Size']=memory_usage_by_variable['Size'].apply(lambda x: obj_size_fmt(x))
    return memory_usage_by_variable

def find_not_matching_columns_new(df,primary_key_list):
    TargetNotMatchingRows = None
    df_final = df.loc[(df['Difference_status'] == 'source_delta') | (df['Difference_status'] == 'target_delta')]
    df_final["Not_Matching_Columns"] = ""
    df=df.loc[(df['Difference_status'] !='source_delta') & (df['Difference_status'] !='target_delta')]
    target_only_df=df.loc[df["Difference_status"] == 'target data mismatch']
    TargetNotMatchingRows=target_only_df

    Notmatching_Columns_List = []
    primary_key=""
    if(len(primary_key_list)>0):

        for index, row in TargetNotMatchingRows.iterrows():
            ID_FilterRows = None
            if (len(primary_key_list) == 1):
                primary_key = primary_key_list[0]
                ID = row[primary_key]
                ID_FilterRows = df.loc[df[primary_key] == ID]
            else:
                if (len(primary_key_list) == 2):
                    ID1 = row[primary_key_list[0]]
                    ID2 = row[primary_key_list[1]]
                    ID_FilterRows = df.loc[(df[primary_key_list[0]] == ID1) & (df[primary_key_list[1]] == ID2)]
                elif (len(primary_key_list) == 3):
                    ID1 = row[primary_key_list[0]]
                    ID2 = row[primary_key_list[1]]
                    ID3 = row[primary_key_list[2]]
                    ID_FilterRows = df.loc[(df[primary_key_list[0]] == ID1) & (df[primary_key_list[1]] == ID2) & (
                                df[primary_key_list[2]] == ID3)]

            if (ID_FilterRows.shape[0] == 2):
                Columns_To_Compare = df.columns.to_list()
                temp_col=[]
                for column in Columns_To_Compare:
                    if (column != 'Difference_status'):
                        source_value = ID_FilterRows[column].unique()
                        if (len(source_value) == 2):
                            temp_col.append(column)
                            Notmatching_Columns_List.append(column)
                ID_FilterRows['Not_Matching_Columns'] = str(temp_col)
                df_final=pd.concat([df_final,ID_FilterRows])

    elif(len(primary_key_list)==0):
        Notmatching_Columns_List.append("Primary Key Is Not Defined To Find Mismatch Columns")



    return [unique(Notmatching_Columns_List),df_final]

def find_not_matching_columns(df,primary_key_list):
    TargetNotMatchingRows = None
    target_only_df=df.loc[df["_merge"] == 'right_only']
    source_only_df = df.loc[df["_merge"] == 'left_only']
    if(source_only_df.shape[0]>target_only_df.shape[0]):
        TargetNotMatchingRows=target_only_df
    else:
        TargetNotMatchingRows = source_only_df
    Notmatching_Columns_List = []
    primary_key=""
    if(len(primary_key_list)>0):

        for index, row in TargetNotMatchingRows.iterrows():
            ID_FilterRows = None
            if (len(primary_key_list) == 1):
                primary_key = primary_key_list[0]
                ID = row[primary_key]
                ID_FilterRows = df.loc[df[primary_key] == ID]
            else:
                if (len(primary_key_list) == 2):
                    ID1 = row[primary_key_list[0]]
                    ID2 = row[primary_key_list[1]]
                    ID_FilterRows = df.loc[(df[primary_key_list[0]] == ID1) & (df[primary_key_list[1]] == ID2)]
                elif (len(primary_key_list) == 3):
                    ID1 = row[primary_key_list[0]]
                    ID2 = row[primary_key_list[1]]
                    ID3 = row[primary_key_list[2]]
                    ID_FilterRows = df.loc[(df[primary_key_list[0]] == ID1) & (df[primary_key_list[1]] == ID2) & (
                                df[primary_key_list[2]] == ID3)]

            if (ID_FilterRows.shape[0] == 2):
                Columns_To_Compare = df.columns.to_list()
                for column in Columns_To_Compare:
                    if (column != '_merge'):
                        source_value = ID_FilterRows[column].unique()
                        if (len(source_value) == 2):
                            Notmatching_Columns_List.append(column)


    elif(len(primary_key_list)==0):
        Notmatching_Columns_List.append("Primary Key Is Not Defined To Find Mismatch Columns")



    return unique(Notmatching_Columns_List)

def find_Not_Matching_Columns_srikanth(source_df_count, target_df_count, MatchingDF, NotMatchingDF, primary_key):
    Delimiter = ","
    ActualDataFrameName = "NotMatchingDF"
    print('------------------------------------------')
    print('*******    Not Matching Records Details   ******')

    FinalNotMatchingRows = pd.DataFrame()
    TargetNotMatchingRows = pd.DataFrame()
    NotMatchingRows = pd.DataFrame()

    if (source_df_count >= target_df_count):
        TargetNotMatchingRows = NotMatchingDF.loc[NotMatchingDF["_merge"] == 'right_only']
    else:
        TargetNotMatchingRows = NotMatchingDF.loc[NotMatchingDF["_merge"] == 'left_only']

    columns = TargetNotMatchingRows.columns
    # print(columns)

    MismatchCols_IssuesCount = []
    for index, row in TargetNotMatchingRows.iterrows():
        columnDataNotmatchlist = []
        print("=============================================================")
        final = []
        UniqueColumns = row[primary_key]
        Join_Query=""
        for ConditionalColumnName in primary_key:
            final.append(ActualDataFrameName + "." + ConditionalColumnName + "  = '" + UniqueColumns[
                ConditionalColumnName] + "' ) AND ")
            Join_Query = 'SELECT * FROM ' + ActualDataFrameName + '' \
                                                                  ' Where ' + str(final).replace(") ",
                                                                                                 "").replace(
                '["', '').replace('", "', "").replace('AND "]', "")
        # print(str(Join_Query))
        Final_Match = ps.sqldf(Join_Query, locals())

        for col in columns:
            FinalViewDF = pd.DataFrame()
            NotMatchingDF2 = Final_Match["" + col + ""]
            NotMatchingDF3 = NotMatchingDF2.drop_duplicates(keep='first')
            NotMatchingDF3_Count = NotMatchingDF3.shape[0]
            mismatch_Column_Records_Count = 0
            if NotMatchingDF3_Count > 1:
                if col != "_merge":
                    # print(col, "Data Mismatched in this Column")
                    columnDataNotmatchlist.append(col)
            else:
                print()
                # print(col,"Data Matched in this Column")
        DataNotmatchlist = ''
        for s in columnDataNotmatchlist:
            DataNotmatchlist += s + ','

        Final_Match['NotMatching_Columns_List'] = DataNotmatchlist
        # print(NotMatching_Columns_List)
        NotMatchingRows = NotMatchingRows.append(Final_Match)

    FinalNotMatchingRows = pd.concat([MatchingDF, NotMatchingRows])

    FinalNotMatchingRows.to_csv("Difference.csv")
    print(FinalNotMatchingRows)
    return FinalNotMatchingRows