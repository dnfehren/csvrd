#opens csv and excel files
#extracts all data

import csv
import xlrd
import os
import MySQLdb
import re

import pprint
pp = pprint.PrettyPrinter(indent=4)

dbconn = MySQLdb.connect(user='root', 
                         passwd='virtual',
                         db='energy_data')
cur = dbconn.cursor()

def find_act_num(block):
  
    col_num = -999
    coordinates = {}

    for row_num in range(0,len(block)):
      try:
        col_num = block[row_num].index('ACCOUNT NUM')
        coordinates['col_num'] = col_num
        coordinates['row_num'] = row_num
        #start_loc.append(col_num)
        #start_loc.append(row_num)
      except ValueError:
        next

    return coordinates

def find_blank_cells(row):

    found_blanks = []

    for c in range(0,len(row)):
        m = re.match('^\s*$', row[c])
        if m:
            found_blanks.append(c)

    return found_blanks

def clean_cols(row):
    #build a string of header names used as db column titles
    cols = []
    #insert_cols = []

    #two regexs, 
    # the first is all white space
    # the second is anything no a-zA-Z0-9 or _
    internal_ws = re.compile('\s')
    cleaner = re.compile('\W')
      
    #loop through the header row list
    for c in row:
      c.strip() #strip right and left white space
      c = internal_ws.sub('_',c) # replace any internal ws with _
      c = cleaner.sub('',c) # remove anything not character
      c = c.lower() #lowercase the string
      cols.append(c) #append the string to the clean header list

    return cols


def header_compare(dbcursor, sheet_col_list):

    present = dbcursor.execute('SHOW TABLES LIKE "data_rows"')

    insert_dict = {}

    if present:
        #get the column titles as they currently exist in the database
        #in comments here http://www.halfcooked.com/mt/archives/000969.html
        dbcursor.execute('SELECT * FROM data_rows LIMIT 1')
        current_sql_cols = [d[0] for d in dbcursor.description]
        
        try:
            current_sql_cols.remove('table_id')
        except ValueError:
            pass

        #print 'SQL = ' + str(len(current_sql_cols))
        #print 'SHT = ' + str(len(sheet_col_list))

        sql_len = len(current_sql_cols)
        sht_len = len(sheet_col_list)

        if set(current_sql_cols) == set(sheet_col_list):
            
            insert_cols_str = ', '.join(sheet_col_list)

            #fordebugging
            #insert_cols_str = 'EQUAL LENS ' + insert_cols_str
            
            insert_dict['sql_len'] = sql_len
            insert_dict['sht_len'] = sht_len
            insert_dict['insert_col_str'] = insert_cols_str

            return insert_dict

        else:
            col_difference = set(sheet_col_list).difference(set(current_sql_cols))
            
            for p in col_difference:
                dbcursor.execute('ALTER TABLE data_rows ADD COLUMN %s VARCHAR(50)' % (p))
                current_sql_cols.append(p)

            insert_cols_str = ', '.join(current_sql_cols)

            #debugging
            #insert_cols_str = 'UNION SET ' + insert_cols_str
            
            insert_dict['sql_len'] = sql_len
            insert_dict['sht_len'] = sht_len
            insert_dict['insert_col_str'] = insert_cols_str

            return insert_dict

    else:
        #join the col names with commas, to be used in insert statements
        insert_cols_str = ', '.join(sheet_col_list)

        #join the col names with commas and data type keywords
        create_cols_str = ' VARCHAR(50), '.join(sheet_col_list)
        create_cols_str = create_cols_str + ' VARCHAR(50)'
        create_cols_str = ('table_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, '
                      + create_cols_str)
   
        dbcursor.execute('CREATE TABLE IF NOT EXISTS data_rows (%s)' % (create_cols_str))

        #debugging
        #insert_cols_str = 'NO TABLE ' + insert_cols_str

        insert_dict['sql_len'] = len(sheet_col_list)
        insert_dict['sht_len'] = len(sheet_col_list)
        insert_dict['insert_col_str'] = insert_cols_str

        return insert_dict


dir_list = os.walk('./test_sheets/20sheets/')

for root,sub,files in dir_list:
    print "root =", root
    #print "dirs =", sub
    #print "files =", files

    for potential_file in files:
    
        file_name, file_extension = os.path.splitext(potential_file)
        
        print 'FILE = ' + file_name

        if file_extension == '.csv':
            #do a csv thing
            pass
        elif file_extension == '.xls':
            wb = xlrd.open_workbook(os.path.join(root,potential_file))
            sh = wb.sheet_by_index(0)
      
            #grabs a 5*5 box starting in the top left corner of the sheet
            header_search_box = []
            for rng in range(0,5):
                header_search_box.append(sh.row_values(rng,0,5))
      
            #pass the box to the function that finds the loc of 'ACCOUNT NUM'
            act_loc = find_act_num(header_search_box)
 
            #get a full col of data, used for length counts later
            c_vals = sh.col_values(act_loc['col_num'])
     
            #check header row for blanks
            header_row = sh.row_values(act_loc['row_num'])
      
            #will store the location of blank cells in the header row
            found_blanks = find_blank_cells(header_row)

            #loop through the list of blanks from the header
            # for each location in the list grab the column
            # beneath it and check for the presence of anything
            # but whitespace
            for c in found_blanks:
                testing_col = sh.col_values(c)
                for r in testing_col:
                    m = re.match('/S',r)
                    if m:
                        print "not actually blank =",m.group()
                    else:
                        pass #some kind of inteligent action should go here

            #assuming that nothing was found in the blank columns
            # delete the blanks from the header row list
            for c in reversed(found_blanks):
                del header_row[c]
            
            sheet_header = clean_cols(header_row)

            sql_cols = header_compare(cur, sheet_header)
            
            #get the data rows beneath the header row
            for row in range(act_loc['row_num'] + 1 ,len(c_vals)):
                
                r_vals = sh.row_values(row)
        
                for r in reversed(found_blanks): #reversed to preserve index
                    del r_vals[r]
        
                #each cell from the row is
                # surrounded with single quotes
                # stripped of all start and end whitespace
                # then the whole list is joined using commas
                row_clean_list = ['"' + str(elem).strip() + '"' for elem in r_vals]
                row_string = ','.join(row_clean_list)

                col_dif = sql_cols['sql_len'] - sql_cols['sht_len']

                #print col_dif

                if col_dif > 0:
                    for n in range(col_dif):
                        row_string = row_string + ', NULL'
                
                #insert the row into the database
                sql = 'INSERT INTO data_rows (%s) VALUES(%s)' % (sql_cols['insert_col_str'], row_string)
                #print sql

                cur.execute(sql)
                print str(cur.rowcount) #insert is not working, why the hell not?
