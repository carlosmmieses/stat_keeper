import csv

class CsvHandler:
  def __init__(self):
    self.file_name = ""
    self.current_file = ""
    
  def open_file(self, file_name):
    self.current_file = open(file_name, "w", newline="")
    
  def close_file(self):
    self.current_file.close()
    
  def write_row_headers(self, columns):
    writer = csv.writer(self.current_file)
    writer.writerow(columns)
    
  def write_row_values(self, values):
    writer = csv.writer(self.current_file)
    writer.writerows(values)