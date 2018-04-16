#!/usr/bin/python
#
# Created on Apr, 2018
# @author: CloudyPotato 
# All rights reserved 

import MySQLdb
import glob, os
import re
import argparse

def get_db_version(connection, version_table):

    cursor = connection.cursor()

    cursor.execute("SELECT * FROM %s" % (version_table,))

    # print all the first cell of the 1st rows
    for row in cursor.fetchall():
        version = row[0]

    extract_version = re.findall(r"\d*\.\d+|\d+", version)

    # use only the version major and minor e.g.  x or x.x 
    # disregard the version patch e.g. x.x.x
    version_nr = float(extract_version[0]) if '.' in extract_version[0] else int(extract_version[0])
    
    connection.commit()
    cursor.close()
    return version_nr

def get_scripts (scripts_dir, db_version):
    scripts_to_run = []
    higher_version = db_version
    
    # change the working directory
    os.chdir(scripts_dir)
    
    for file in glob.glob("*.sql"):
    
        # normalize file name
        # use only the version major and minor e.g.  x or x.x    
        # disregard the version patch e.g. x.x.x
        # remove all '_' and '-' in the file name
        norm_filename = re.findall(r"\d*\.\d+|\d+", 
        file[0: file.find('.sql')].replace ('_', '').replace('-', ''))

        script_version = float(norm_filename[0]) if '.' in norm_filename[0] else int(norm_filename[0])
 
        if script_version > db_version:
            scripts_to_run.append(file)

        if script_version > higher_version:
            higher_version = script_version

    return scripts_to_run, higher_version


def execute_sql_scripts(connection, scripts_dir, scripts):
    # change the working directory
    os.chdir(scripts_dir)

    for script in scripts:
        print "Executing: " + script
        cursor = connection.cursor()
 
        # asume there is one command per line
        # Note: the command is not split across multiple lines
        for line in open(script): 
            cursor.execute(line)
                
        connection.commit()
        cursor.close()

def update_db_version(connection, db_name, version_table, new_version):
    cursor = connection.cursor()
    
    # make sure the initial DB is used
    cursor.execute("USE %s" % (db_name,))
    cursor.execute("UPDATE %s SET version=%s" % (version_table, new_version))
    connection.commit()
    cursor.close()

def main():
    parser = argparse.ArgumentParser(description='Script to identify and execute SQL scripts.')
    parser.add_argument('-d','--scripts_dir', default=None, help='Full path to the scripts folder')
    parser.add_argument('-u','--user', default=None, help='Username for the database')
    parser.add_argument('-m','--host', default=None, help='Host of the database')
    parser.add_argument('-n','--db_name', default=None, help='Name for the database')
    parser.add_argument('-p','--password', default=None, help='Password for the database')

    args = parser.parse_args()

    scripts_dir = args.scripts_dir
    user = args.user
    host = args.host
    db_name = args.db_name
    password = args.password
   
    try:
        connection = MySQLdb.connect(host = host,
                         user = user,
                         passwd = password,
                         db = db_name)

        version_table = "versionTable"

        db_version = get_db_version(connection, version_table)
        print "Current version: " + str(db_version)

        scripts, higher_version = get_scripts(scripts_dir, db_version)
 
        execute_sql_scripts (connection, scripts_dir, scripts)

        #higher_version=58
        if higher_version != db_version:
            update_db_version(connection, db_name, version_table, str(higher_version))
            print "Updated version: " + str(higher_version)
        connection.close()

    except MySQLdb.Error as err:
        print("Something went wrong: {}".format(err))

if __name__ == '__main__':
        main()
