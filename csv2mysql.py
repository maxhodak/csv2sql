#!/usr/bin/env python
# encoding: utf-8
"""
csv2mysql

The MIT License

Copyright (c) 2009 Max Hodak

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import sys
import getopt
import csv
import os

help_message = '''
csv2mysql generates valid MySQL CREATE TABLE and LOAD DATA INFILE statements
for a given csv file.  The csv *must* include column names in the first now.
Types are inferred by examining a column's contents.  Types are generated as
compressed as possible: for example, if the longest string in a varchar column
is 27 characters long, varchar(27) will be used; this might cause truncation
if a new row is later added with a 28-character entry in that column.
'''

class Usage(Exception):
  def __init__(self, msg):
    self.msg = msg

  
class csv2mysql(object):
  
  def __init__(self, filename):
    super(csv2mysql, self).__init__()
    
    self.filename = filename
    tmp = filename.split('.')[0]
    if '/' in tmp:
      tmp = tmp.split('/')[-1:][0]
    self.tablename = tmp
    self.csvfile = open(filename)
    
    self.dialect = csv.Sniffer().sniff(self.csvfile.read(128))
    self.csvfile.seek(0)
    
    self.csvreader = csv.reader(self.csvfile, self.dialect)
    self.headers = self.csvreader.next()
    self.headers = [header.split(':')[0] for header in self.headers]
    self.types = dict([(header,'INT(11)') for header in self.headers])
  
  def isInteger(self, x):
    if x == '' or x == None:
      return True
    try:
      foo = int(x)
      return True
    except:
      return False
  
  def generate(self):
    for row in self.csvreader:
      i = 0
      for header in self.headers:
        col = row[i]
        if self.types[header] == 'INT(11)':
          if self.isInteger(col) == False:
            self.types[header] = 'VARCHAR(255)'
        i += 1
  
  def buildCreateTable(self):
    sql = "CREATE TABLE IF NOT EXISTS `"+self.tablename+"` (\n"
    for header in self.headers:
      sql = sql+"  `"+header+"` \t "+self.types[header]+",\n"
    sql = sql[:-2]
    sql += "\n);"
    return sql
  
  def buildInserts(self):
    sql = ""
    self.csvfile.seek(0)
    self.csvreader = csv.reader(self.csvfile, self.dialect)
    self.csvreader.next()
    for row in self.csvreader:
      sql += "INSERT INTO `"+self.tablename+"` VALUES ("
      for col in row:
        sql += "'"+col.replace('"', '\"').replace("'", "\\'")+"',"
      sql = sql[:-1]
      sql += ");\n"
    return sql

def main(argv=None):
  if argv is None:
    argv = sys.argv

  try:
    try:
      opts, args = getopt.getopt(argv[2:], 
                                  "hv", 
                                  ["help"])
    except getopt.error, msg:
      raise Usage(msg)
  
    for option, value in opts:
      if option == "-v":
        verbose = True
      if option in ("-h", "--help"):
        raise Usage(help_message)
  
  except Usage, err:
    print >> help_message
    print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
    return 2
  
  try:
    try:
      fname = argv[1]
    except IndexError:
      raise Usage("No input file supplied")
      return 2
    try:      
      builder = csv2mysql(fname)
    except IOError:
      raise Usage("File '"+fname+"' not found or invalid.")
      return 2
    
    oname = fname.split('.')[0]
    if '/' in fname:
      oname = oname.split('/')[-1:][0]
    output = open(oname+'.sql','w+')
    builder.generate()

    print >> output, builder.buildCreateTable()
    print >> output, builder.buildInserts()
    
  except Usage, err:
    print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
    return 2    
  
if __name__ == "__main__":
  sys.exit(main())