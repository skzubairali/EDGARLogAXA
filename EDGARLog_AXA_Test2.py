import pandas as pd
import numpy as np

def check_sample_rows_and_schema(dataframe, row_count=3):
    print("Following are the top " + str(row_count) + " sample rows from dataframe:" + "\n")
    print(dataframe.head(row_count))
    print("Following are the types of columns within the dataframe:")
    print(dataframe.dtypes)
    print("\n" + "\n" + "\n")
    
def check_nulls_presence(dataframe, column):
    if dataframe[column].isnull().values.any():
        print(column + " has null values. Hence exiting the process. Please reach out to client to how to handle the nulls")
        exit(1)
    else:
        print(column + " does not have any null values")

def read_input_file(file_path):
    dataframe = pd.read_csv(file_path, low_memory=False)
    print(str(len(dataframe.index))+" number of rows are ingested from log file to pandas dataframe for analysis")
    check_sample_rows_and_schema(dataframe)
    return dataframe

def select_necessary_columns(dataframe, columns):
    return dataframe[columns]
    
def transformed_columns_for_download_identification(dataframe):
    print("adding the transformed columns to the dataframe" + "\n")
    dataframe['transformed_downloaded_file_size'] = np.where(dataframe['code'].astype(str).str[0] == '2', dataframe['size'], 0)
    dataframe['transformed_downloaded_file_count_indicator'] = np.where(dataframe['code'].astype(str).str[0] == '2', 1, np.nan)
    dataframe["timestamp"] = pd.to_datetime(dataframe['date'] + dataframe['time'], format='%Y-%m-%d%H:%M:%S')
    dataframe['timestamp_min'] = dataframe.groupby('ip')['timestamp'].transform('min')
    print("adding the transformed columns to the dataframe completed" + "\n" + "\n" + "\n")
    return dataframe
    
def sessionize_data(dataframe, partition_columns, session_creation_base_column, final_columns):
    print("sessionizing the dataframe" + "\n")
    check_nulls_presence(dataframe, session_creation_base_column)
    dataframe[session_creation_base_column + '_min'] = dataframe.groupby(partition_columns)[session_creation_base_column].transform('min')
    dataframe['timedelta_wrt_min_timestamp'] = (dataframe[session_creation_base_column] - dataframe[session_creation_base_column + '_min']).astype('timedelta64[m]')
    dataframe['session_bucket_int'] = np.where(dataframe['timedelta_wrt_min_timestamp'] == 0, 1, np.where(dataframe['timedelta_wrt_min_timestamp']%30 != 0, ((dataframe['timedelta_wrt_min_timestamp']/30).astype(int))+1, (dataframe['timedelta_wrt_min_timestamp']/30).astype(int)))
    dataframe['session_bucket'] = dataframe.groupby(partition_columns)['session_bucket_int'].rank(method='dense').astype(int)   
    check_sample_rows_and_schema(dataframe)
    print("sessionizing the dataframe completed" + "\n" + "\n" + "\n")
    return dataframe[final_columns]
    
def aggregate_dataframe(columns, dataframe):
    print("aggregating the dataframe" + "\n")
    dataframe_grouped = dataframe.groupby(columns)
    final_aggregated_dataframe = dataframe_grouped.agg(total_downloaded_files_size = pd.NamedAgg(column='transformed_downloaded_file_size', aggfunc='sum'), total_downloaded_files_count = pd.NamedAgg(column='transformed_downloaded_file_count_indicator', aggfunc='count')).reset_index() 
    check_sample_rows_and_schema(final_aggregated_dataframe)
    print("aggregating the dataframe completed" + "\n" + "\n" + "\n")
    return final_aggregated_dataframe
    

def top_number_sessions_per_user_basis_column(dataframe, partition_columns, sort_column, number_of_rows=10):
    if sort_column == 'total_downloaded_files_size':
        print("creating report for user wise top sessions basis downloaded file size" + "\n")
        dataframe['rank_wrt_downloaded_files_size'] = dataframe.groupby(partition_columns)[sort_column].rank(method='first', ascending=False).astype(int)
        print("creating report for user wise top sessions basis downloaded file size completed" + "\n" + "\n" + "\n")
        return dataframe[dataframe['rank_wrt_downloaded_files_size'] <= number_of_rows].sort_values([",". join(partition_columns),'rank_wrt_downloaded_files_size'])[['ip','session_bucket']]
    else:
        print("creating report for user wise top sessions basis downloaded file count" + "\n")
        dataframe['rank_wrt_downloaded_files_count'] = dataframe.groupby(partition_columns)[sort_column].rank(method='first', ascending=False).astype(int)
        print("creating report for user wise top sessions basis downloaded file size completed" + "\n" + "\n" + "\n")
        return dataframe[dataframe['rank_wrt_downloaded_files_count'] <= number_of_rows].sort_values([",". join(partition_columns),'rank_wrt_downloaded_files_count'])[['ip','session_bucket']]
        
def write_dataframe_to_csv(dataframe, file_path):
    dataframe.to_csv(file_path,index=False)
    print(str(len(dataframe.index))+" number of rows are written to csv file")
        
    
def main():
    # ingesting the file in to pandas dataframe.
    input_dataframe = read_input_file(r'C:\Users\suryapr\Desktop\log20170201\log20170201.csv')
    
    #select only necessary columns from dataframe which are required for analysis.
    dataframe_to_be_analysed = select_necessary_columns(input_dataframe, ['ip','date','time','code','size'])
    
    #adding the transformed columns to the dataframe that help us in identifying downloaded files attributes.
    transformed_dataframe = transformed_columns_for_download_identification(dataframe_to_be_analysed)
    
    #sessionize the data to create session buckets per each user identified by ip 
    sessionized_dataframe = sessionize_data(transformed_dataframe, ['ip'], 'timestamp', ['ip','session_bucket','transformed_downloaded_file_size','transformed_downloaded_file_count_indicator'])
    
    # generating the aggregated columns of total downloaded file size and total downloaded file count per user and session
    aggregated_dataframe  = aggregate_dataframe(['ip','session_bucket'], sessionized_dataframe)
    
    # getting top ten sessions per user basis total_downloaded_file_count and writing it to an output file
    
    write_dataframe_to_csv(top_number_sessions_per_user_basis_column(aggregated_dataframe, ['ip'], 'total_downloaded_files_size'), r'C:\Users\suryapr\Desktop\top_user_wise_sessions_basis_size.csv')
    
    # getting top ten sessions per user basis total_downloaded_file_count and writing it to an output file
    
    write_dataframe_to_csv(top_number_sessions_per_user_basis_column(aggregated_dataframe, ['ip'], 'total_downloaded_files_count'), r'C:\Users\suryapr\Desktop\top_user_wise_sessions_basis_count.csv')
    

    main()    
    
    
