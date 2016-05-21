# -*- coding: utf-8 -*-
"""
pandas df to tableu data extract

publish data extract to server
"""

import os
import pandas as pd

from tableausdk import *
from tableausdk.Server import *
from tableausdk.Extract import *

def to_tde(df, extractname, tablename, data_type, index = False, new_extract = False, append = True) :
    '''
    change pandas dataframe to tableau data extract
    '''
    table_definition_map = { 
                            'Bool' :    Type.BOOLEAN,
                            'Integer':  Type.INTEGER,
                            'Double':   Type.DOUBLE,
                            'Date':     Type.DATE,
                            'DateTime': Type.DATETIME,
                            'Unicode':  Type.UNICODE_STRING,
                            'Char':     Type.CHAR_STRING
                            }
    value_set_map = {
                  Type.BOOLEAN:        lambda row, col_num, value: row.setBoolean( col_num, bool(value) ),
                  Type.INTEGER:        lambda row, col_num, value: row.setInteger( col_num, int(value) ),
                  Type.DOUBLE:         lambda row, col_num, value: row.setDouble( col_num, float(value) ),
                  Type.UNICODE_STRING: lambda row, col_num, value: row.setString( col_num, unicode(value) ),
                  Type.CHAR_STRING:    lambda row, col_num, value: row.setCharString( col_num, value ),
                  Type.DATE:           lambda row, col_num, value: row.setDate(col, value.year, value.month, value.day),
                  Type.DATETIME:       lambda row, col_num, value: row.setDateTime( col_num,  value.year,  value.month,  value.day,
                                                                                  value.hour,  value.minute,  value.second,
                                                                                  value.microsecond/100 )
                  }
    
    if index:
        df_new = df.reset_index()
    else:
        df_new = df        
        
    if extractname[-4:] != '.tde':
        extractname += '.tde'    
    
    if new_extract & (extractname in os.listdir()):
            os.remove(extractname)
    extract_to_save = Extract(extractname)
            
    if extract_to_save.hasTable(tablename) & append:
        table_to_save = extract_to_save.openTable(tablename)
        table_definition = table_to_save.getTableDefinition()
    else:
        table_definition = TableDefinition()
        for colname in df_new.columns:
            type_code = table_definition_map[data_type.get(colname, 'Unicode')]
            table_definition.addColumn(colname, type_code)
        table_to_save = extract_to_save(tablename, table_definition)
    
    for df_row in df_new.iterrows():
        new_row = Row(table_definition)
        for col_num, (col_val, null_col) in enumerate(zip(df_row, df_row.isnull())):
            if null_col:
                new_row.set(col_num)
            else:
                value_set_map[table_definition.getColumnType(col_num)](new_row, col_num, col_val)
        table_to_save.insert(new_row)
    
    extract_to_save_close()
    ExtractAPI.cleanup()
                                       
    
def publish_tde(server, username, password, siteID, extractname, project, publishname, overwrite = True) :
    '''
    publish tde to server
    basically some changes to the sample code form tableau
    '''
    try:
        # Initialize Tableau Server API
        ServerAPI.initialize()
    
        # Create the server connection object
        serverConnection = ServerConnection()
    
        # Connect to the server
        serverConnection.connect(server, username, password, siteID)
    
        # Publish order-py.tde to the server under the default project with name Order-py
        serverConnection.publishExtract(extractName, project, publishname, overwrite)
    
        # Disconnect from the server
        serverConnection.disconnect()
    
        # Destroy the server connection object
        serverConnection.close()
    
        # Clean up Tableau Server API
        ServerAPI.cleanup()
    
    except TableauException, e:
        # Handle the exception depending on the type of exception received
    
        errorMessage = "Error: "
    
        if e.errorCode == Result.INTERNAL_ERROR:
            errorMessage += "INTERNAL_ERROR - Could not parse the response from the server."
    
        elif e.errorCode == Result.INVALID_ARGUMENT:
            errorMessage += "INVALID_ARGUMENT - " + e.message
    
        elif e.errorCode == Result.CURL_ERROR:
            errorMessage += "CURL_ERROR - " + e.message
    
        elif e.errorCode == Result.SERVER_ERROR:
            errorMessage += "SERVER_ERROR - " + e.message
    
        elif e.errorCode == Result.NOT_AUTHENTICATED:
            errorMessage += "NOT_AUTHENTICATED - " + e.message
    
        elif e.errorCode == Result.BAD_PAYLOAD:
            errorMessage += "BAD_PAYLOAD - Unknown response from the server. Make sure this version of Tableau API is compatible with your server."
    
        elif e.errorCode == Result.INIT_ERROR:
            errorMessage += "INIT_ERROR - " + e.message
    
        else:
            errorMessage += "An unknown error occured."
    
        print errorMessage

















