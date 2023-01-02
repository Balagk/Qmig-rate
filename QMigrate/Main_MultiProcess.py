import time
import PySimpleGUI as sg
import numpy as np
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
import Utilities
from Utilities import read_dump_file
from multiprocessing import Pool
from multiprocessing import freeze_support
from functools import partial


column_names = ["TableName", "Source_Count_Before_Remove_Dup", "Target_Count_Before_Remove_Dup",
                "Source_Count_After_Remove_Dup", "Target_Count_After_Remove_Dup", "Source_Dup_count",
                "Target_Dup_count", "Matching", "Data_NotMatching", "Source_Delta", "Target_Delta", "Status","MisMatch Columns","% Match"]

overview = pd.DataFrame(columns=column_names)

def ProcessingFiles(data):
    try:
        # print(sorceDBconn)
        row = data[1]
        Source_DataDF = pd.DataFrame()
        Target_DataDF = pd.DataFrame()
        MappingSheetName = str(row.MAPPINGSHEET)
        print("############################# " + MappingSheetName + " #############################")
        ResourceType = str(row.RESOURCE)
        print("Resource Type: " + ResourceType)
        SourceTableName = str(row.SOURCE_TABLE_NAME)
        print("Source Table/Dump Name: " + SourceTableName)
        TargetTableName = str(row.TARGET_TABLE_NAME)
        Source_SQL = str(row.SOURCE_SQL)
        Target_SQL = str(row.TARGET_SQL)
        print("Target Table/Dump Name: " + TargetTableName)


        if (ResourceType.upper() == 'DUMP'):
            Source_DataDF = read_dump_file('SOURCE', SourceTableName)
            Target_DataDF = read_dump_file('TARGET', TargetTableName)

        elif (ResourceType.upper() == 'DATABASE'):

            Source_DataDF = Utilities.read_database_table('SOURCE', SourceTableName, Source_SQL)
            Target_DataDF = Utilities.read_database_table('TARGET', TargetTableName, Target_SQL)
        else:
            print("ERROR: Resource Type Not Provided For The Table " + MappingSheetName)

        Source_DataDF_Count_Before_Drop_Dup = Source_DataDF.shape[0]
        Target_DataDF_Count_Before_Drop_Dup = Target_DataDF.shape[0]
        sourceDupsDf = Source_DataDF[Source_DataDF.duplicated(keep='first')]
        targetDupsDf = Target_DataDF[Target_DataDF.duplicated(keep='first')]
        sourceDupsDf.to_csv("Results\\Duplicates\\Source\\" + MappingSheetName + "_duplicates.csv")
        targetDupsDf.to_csv("Results\\Duplicates\\Target\\" + MappingSheetName + "_duplicates.csv")
        Source_DataDF = Source_DataDF.drop_duplicates(keep='first')
        Target_DataDF = Target_DataDF.drop_duplicates(keep='first')
        Source_DataDF_Count_After_Drop_Dup = Source_DataDF.shape[0]
        Target_DataDF_Count_After_Drop_Dup = Target_DataDF.shape[0]
        sourceDup_Count = str(Source_DataDF_Count_Before_Drop_Dup - Source_DataDF_Count_After_Drop_Dup)
        targetDup_Count = str(Target_DataDF_Count_Before_Drop_Dup - Target_DataDF_Count_After_Drop_Dup)

        # Apply Transofrmation Logics On Source Data
        mappingSheetDF = pd.read_excel("MappingSheet.xlsx", sheet_name=MappingSheetName).fillna('NULL')
        TransormationLogicsDF = mappingSheetDF[mappingSheetDF['TRANSORMATION LOGIC'] != 'NULL']
        PrimaryKeysDF = mappingSheetDF[mappingSheetDF['PRIMARYKEY'] == 'Y']
        PrimaryKeysList = PrimaryKeysDF["TARGET COLUMNS"].to_list()
        Target_Columns=(mappingSheetDF['TARGET COLUMNS'].to_list())
        if "NULL" in Target_Columns:
            Target_Columns.remove('NULL')

        if (TransormationLogicsDF.shape[0] > 0):
            for index, row in TransormationLogicsDF.iterrows():
                transformation_logic = row['TRANSORMATION LOGIC']
                logicList = str(transformation_logic).split(":")
                print("T/F Logic: " + str(transformation_logic))
                Source_logic_column = logicList[0]
                logic_function = logicList[1]
                Target_logic_column = logicList[2]

                RunLogic_Function = getattr(Utilities, logic_function)
                Source_DataDF = RunLogic_Function(Source_DataDF, Source_logic_column, Target_logic_column)
        # Rename column names if target has different name for non logic columns
        NonTransormationLogicsDF = mappingSheetDF[mappingSheetDF['TRANSORMATION LOGIC'] == 'NULL']
        if (NonTransormationLogicsDF.shape[0] > 0):
            for index, row in NonTransormationLogicsDF.iterrows():
                source_column = str(row['SOURCE COLUMNS'])
                target_column = str(row['TARGET COLUMNS'])
                if (source_column != target_column):
                    Source_DataDF = Source_DataDF.rename(columns={source_column: target_column})

        Source_DataDF=Source_DataDF[Target_Columns]
        Target_DataDF=Target_DataDF[Target_Columns]
        NotMatchingData = Utilities.dataframe_difference(Source_DataDF, Target_DataDF)
        MatchingData = Utilities.dataframe_difference(Source_DataDF, Target_DataDF, 'both')
        Reason=""
        # Reason = Utilities.find_not_matching_columns(NotMatchingData, PrimaryKeysList)
        # Utilities.find_Not_Matching_Columns_new(Source_DataDF.shape[0], Target_DataDF.shape[0], MatchingData, NotMatchingData, PrimaryKeysList)


        NotMatchingData = NotMatchingData.rename(columns={'_merge': 'Difference_status'})
        finaldiff=None
        if(len(PrimaryKeysList)>=1):
            if (NotMatchingData.shape[0] > 0):
                NotMatchingData['Difference_status'] = NotMatchingData['Difference_status'].str.replace('left_only',
                                                                                                        'Source_only')
                NotMatchingData['Difference_status'] = NotMatchingData['Difference_status'].str.replace('right_only',
                                                                                                        'Target_only')
                NotMatchingData_SourceOnly = NotMatchingData[NotMatchingData['Difference_status'] == 'Source_only']
                NotMatchingData_TargetOnly = NotMatchingData[NotMatchingData['Difference_status'] == 'Target_only']

                ###########////practice
                result_diff_delta=Utilities.dataframe_difference1(NotMatchingData_SourceOnly,NotMatchingData_TargetOnly,PrimaryKeysList)
                result_diff_dataMismatch= Utilities.dataframe_difference1(NotMatchingData_SourceOnly,
                                                                    NotMatchingData_TargetOnly, PrimaryKeysList,"both")
                result_diff_delta['_merge'] = result_diff_delta['_merge'].str.replace(
                    'left_only',
                    'source_delta')
                result_diff_delta['_merge'] = result_diff_delta['_merge'].str.replace(
                    'right_only',
                    'target_delta')
                result_diff_delta.drop(['Difference_status_SOURCE','Difference_status_TARGET'], inplace=True, axis=1)
                result_diff_dataMismatch.drop(['Difference_status_SOURCE','Difference_status_TARGET'], inplace=True, axis=1)
                result_diff_dataMismatch['_merge'] = result_diff_dataMismatch['_merge'].str.replace(
                    'both',
                    'data mismatch')
                NotMatchingData=pd.concat([result_diff_dataMismatch,result_diff_delta])
                NotMatchingData = NotMatchingData.rename(columns={'_merge': 'Difference_status'})

                NotMatchsource_cols = [col for col in NotMatchingData.columns if '_SOURCE' in col]
                NotMatchsource_cols.extend(PrimaryKeysList)
                NotMatchsource_cols.append('Difference_status')
                NotMatchingData_src=(NotMatchingData[NotMatchsource_cols])#.assign(Difference_status='source data mismatch')
                # NotMatchingData_src['Difference_status'] = 'source data mismatch'
                NotMatchingData_src = NotMatchingData_src[NotMatchingData_src.Difference_status != 'target_delta']
                NotMatchingData_src['Difference_status'] = NotMatchingData_src['Difference_status'].str.replace(
                    'data mismatch',
                    'source data mismatch')

                NotMatchtarget_cols = [col for col in NotMatchingData.columns if '_TARGET' in col]
                NotMatchtarget_cols.extend(PrimaryKeysList)
                NotMatchtarget_cols.append('Difference_status')
                NotMatchingData_trg = (NotMatchingData[NotMatchtarget_cols])#.assign(Difference_status='target data mismatch')
                NotMatchingData_trg = NotMatchingData_trg[NotMatchingData_trg.Difference_status != 'source_delta']
                NotMatchingData_trg['Difference_status'] = NotMatchingData_trg['Difference_status'].str.replace(
                    'data mismatch',
                    'target data mismatch')

                # NotMatchingData_trg['Difference_status'] = 'target data mismatch'
                NotMatchingData_src.columns = NotMatchingData_src.columns.str.replace("_SOURCE", "")
                NotMatchingData_trg.columns = NotMatchingData_trg.columns.str.replace("_TARGET", "")
                finaldiff=pd.concat([NotMatchingData_src,NotMatchingData_trg])
                # finaldiff.to_csv("Results\\Difference\\" + MappingSheetName + "_difference.csv", index=False)


        else:
            NotMatchingData['Difference_status'] = NotMatchingData['Difference_status'].str.replace('left_only',
                                                                                                    'Source_only')
            NotMatchingData['Difference_status'] = NotMatchingData['Difference_status'].str.replace('right_only',
                                                                                                    'Target_only')
            NotMatchingData.to_csv("Results\\Difference\\" + MappingSheetName + "_difference.csv", index=False)


        if(len(PrimaryKeysList)==0):
            Reason = "Primary Key Is Not Defined To Find Mismatch Columns"
        else:
            df_diff = finaldiff.loc[(finaldiff['Difference_status'] != 'source_delta') & (finaldiff['Difference_status'] != 'target_delta')]
            if(df_diff.shape[0]>1):
                Reason_columnsNotMatchList = Utilities.find_not_matching_columns_new(finaldiff, PrimaryKeysList)
                Reason = Reason_columnsNotMatchList[0]
                Reason_columnsNotMatchList[1].to_csv("Results\\Difference\\" + MappingSheetName + "_difference.csv",
                                                     index=False)
            else:
                finaldiff.to_csv("Results\\Difference\\" + MappingSheetName + "_difference.csv",
                                                     index=False)




        MatchingDataCount = str(MatchingData.shape[0])
        NotMatchingDataCountDF = NotMatchingData[NotMatchingData['Difference_status'] == 'data mismatch']
        NotMatchingDataCoun = str(NotMatchingDataCountDF.shape[0])
        Source_DeltaDF = NotMatchingData[NotMatchingData['Difference_status'] == 'source_delta']
        Source_Delta_Count = str(Source_DeltaDF.shape[0])
        Target_DeltaDF = NotMatchingData[NotMatchingData['Difference_status'] == 'target_delta']
        Target_Delta_Count = str(Target_DeltaDF.shape[0])
        Status = ""
        if(len(PrimaryKeysList)==0):
            if(Source_DataDF_Count_After_Drop_Dup>Target_DataDF_Count_After_Drop_Dup):
                Source_Delta_Count=Source_DataDF_Count_After_Drop_Dup-Target_DataDF_Count_After_Drop_Dup
                Target_Delta_Count='0'
                NotMatchingDataCoun = Source_DataDF_Count_After_Drop_Dup-MatchingData.shape[0]-Source_Delta_Count
            else:
                Target_Delta_Count = Target_DataDF_Count_After_Drop_Dup-Source_DataDF_Count_After_Drop_Dup
                Source_Delta_Count = '0'
                NotMatchingDataCoun = Target_DataDF_Count_After_Drop_Dup - MatchingData.shape[0]-Target_Delta_Count

        if (int(Source_Delta_Count) == 0 and int(Target_Delta_Count)==0 and int(NotMatchingData.shape[0]) == 0 and int(
                sourceDup_Count) == 0 and int(targetDup_Count) == 0):
            Status = "Matching"
        else:
            Status = "Not Matching"
        PercentageMatch=None
        if(Source_DataDF_Count_After_Drop_Dup>=Target_DataDF_Count_After_Drop_Dup):
            PercentageMatch =(int(MatchingDataCount)/Target_DataDF_Count_After_Drop_Dup)*100
        else:
            PercentageMatch = (int(MatchingDataCount) / Source_DataDF_Count_After_Drop_Dup) * 100


        overview_DF = pd.DataFrame([[MappingSheetName, str(Source_DataDF_Count_Before_Drop_Dup),
                                     str(Target_DataDF_Count_Before_Drop_Dup), str(Source_DataDF_Count_After_Drop_Dup),
                                     str(Target_DataDF_Count_After_Drop_Dup), sourceDup_Count, targetDup_Count,
                                     MatchingDataCount, NotMatchingDataCoun, Source_Delta_Count, Target_Delta_Count,
                                     Status, str(Reason),str(PercentageMatch)]], columns=column_names)
        # global overview
        # overview = pd.concat([overview, overview_DF.applymap(str)])

        # del Source_DataDF
        # del Target_DataDF
        # gc.collect()
        # print(memory_usage())
        return overview_DF

    except Exception as e:
        row = data[1]
        print("Failed Executing Table: "+str(row.MAPPINGSHEET))
        print(e)


if __name__ == '__main__':
    freeze_support()
    print("########### Execution Started ###########")
    start_time = time.time()
    DriverSheetDF = pd.read_excel("Driver.xlsx", sheet_name="Driver")
    DriverSheetDF = DriverSheetDF[DriverSheetDF['RUN_CONTROL'] == 'YES']
    DriverSheetDF.fillna("NULL", inplace=True)
    print("Total Tables Selected For Recon: " + str(DriverSheetDF.shape[0]))

    # listOfDFRows = DriverSheetDF.to_numpy().tolist()
    pool = Pool(5)  # Create a multiprocessing Pool
    DbSourceConnection="DB_Source_Connection"
    result=list((pool.map(ProcessingFiles, DriverSheetDF.iterrows())))

    #Send DB connections in the multiprocess pool
    # result = list((pool.map(partial(ProcessingFiles,sorceDBconn=DbSourceConnection), DriverSheetDF.iterrows())))
    pool.close()
    pool.join()
    overview = pd.concat(result)
    overview.index = np.arange(1, len(overview) + 1)
    overview.to_csv("Results\\Overview.csv", index=False)
    overview_html = overview.to_html(table_id="customers")
    OverviewDF_Matching_Count = (overview.loc[overview['Status'] == "Matching"]).shape[0]
    OverviewDF_Not_Matching_Count = (overview.loc[overview['Status'] == "Not Matching"]).shape[0]
    # write html to file
    text_file = open("Results\\Overview.html", "w")
    html_text = Utilities.html_syntax(OverviewDF_Matching_Count, OverviewDF_Not_Matching_Count, overview_html)
    text_file.write(html_text)
    text_file.close()

    end_time = time.time()
    TimeTaken = Utilities.convert(end_time - start_time)
    print('Time Taken For Execution:' + str(TimeTaken))

    print("##################### Execution Completed in " + str(TimeTaken) + " ################")
    sg.Popup('Execution completed in ' + str(TimeTaken))







