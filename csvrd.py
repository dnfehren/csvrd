import csv
import xlrd

class EasyCsvReader(object):
    
    def __init__(self, open_str):
        self.raw_reader = csv.reader(open(open_str, 'rb'))

        zip_sheet = zip(self.raw_reader)

        working_sheet = []
        
        for tuple_row in zip_sheet:
            working_sheet.append(tuple_row[0])

        self.easy_csv = working_sheet


    def __iter__(self):
        for row in self.easy_csv:
            yield row
        

    def row_values(self, choice_num):
        
        working_row = self.easy_csv[choice_num]
        
        return working_row


    def col_values(self, choice_num):
        
        working_col = []
        
        for row in self.easy_csv:
            working_col.append(row[choice_num])
        
        return working_col
