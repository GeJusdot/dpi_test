#!/usr/sbin/env python
# -*- coding: UTF-8 -*-
import os
import logging
import gzip
import hashlib
import shutil
import time
import sys
import traceback
import ConfigParser
from logging.config import fileConfig
from threadpool import ThreadPool, makeRequests

def un_gz(file_name): 
    data = '' 
    try:
        g_file = gzip.GzipFile(file_name)  
        data = g_file.read()
    except Exception as e: 
        logger.critical("un gzip log file[%s] error [%s]", file_name, e)
    finally:
        g_file.close()
    return data

def md5sum(filedata):          
    fmd5 = hashlib.md5(filedata)  
    return fmd5.hexdigest()

def check_sum(file, ungz_log):
    if not ungz_log:
        logger.warning("un gzip log file is empty")
        return False
    f_sgn = file.replace(".txt.gz", ".sgn")
    if not os.path.isfile(f_sgn):
        logger.warning("sgn file not exisit [%s]", f_sgn)
        return False
    f_md5 = ''
    with open(f_sgn, "r") as fp:
        for line in fp.readlines():
            if line.strip().startswith("md5:"):
                f_md5 = line.strip().split(":")[-1] # use lstrip() md error
    new_md5 = md5sum(ungz_log)
    if new_md5.lower() == f_md5.lower():
        logger.debug("check sum pass[%s]", file)
        return True
    logger.warning("md5 check failed, file[%s], [%s]!=[%s]", file, f_md5, new_md5)
    return False
    

def get_all_files(path):
    l_files=[]
    ll_files=[]
    if os.path.isdir(path):
        l_files = os.listdir(path)
    for one in l_files:
        if one.endswith(cp.get('app','logfile_suffix')):
            ll_files.append(one)
    logger.debug("all gzip file is [%s]", " ".join(ll_files))
    return ll_files


def read_log_to_db(log, db, log_type):
    data = log.split("\n")
    return db.insert(log_type,data)

def do_work(path, file, db, log_type):
    logger.info("begin exec file[%s]", file)
    ungz_log = un_gz(path + file)
    
    if not check_sum(path + file, ungz_log):
        return False
    
    #assert True == check_sum(path + file, ungz_log)
    if True == read_log_to_db(ungz_log, db, log_type):
        logger.debug("read_log_to_db done [%s]", file)
        bak_path = cp.get('app','logfile_bak_path') + log_type + "/"
        shutil.move(path+file, bak_path + file)
        sgn_file = file.replace(cp.get('app','logfile_suffix'), cp.get('app', 'sumfile_suffix'))
        shutil.move(path+sgn_file, bak_path+sgn_file)
        logger.info("bakup origin log [%s] and [%s] to [%s]", file, sgn_file,  bak_path)
        return True
    else:
        return False 

def print_result(request, result):
    logger.info("Result from request #%s: %r" , request.requestID, result) 

def handle_exception(request, exc_info):
    if not isinstance(exc_info, tuple):
        logger.critical("Sys Exception occured in request #%s:%s" , request.requestID, exc_info)
        raise SystemExit
    logger.critical("Exception occured in request #%s: %s" , request.requestID, exc_info) 

def get_request_data(log_type, db):
    logger.debug("starting operate http log...")
    dir = cp.get('app','logfile_path') + log_type + '/'
    all_files = get_all_files(dir)
    data = [(None,{'path':dir, 'file':filename, 'db':db, 'log_type':log_type}) \
            for filename in all_files]
    requests = makeRequests(do_work, data, print_result, handle_exception)
    return requests
           
def main(db):
    try:
        requests = []
        list_log_type = ['http_log', 'flow_log', 'account_log']
        for log_type in list_log_type:
            requests.extend(get_request_data(log_type, db))
        pool = ThreadPool(cp.getint('app', 'thread_nums'))
        [pool.putRequest(req) for req in requests]
        pool.wait()
    except Exception as e:
        logger.critical("Expection occur, produce exits[%s]", e)
        traceback.print_exc()



if __name__ == '__main__':
    starttime = time.time()
    logger.info("########\nstarting...")
    fileConfig('conf/log.conf')
    logger=logging.getLogger("root")
    cp = ConfigParser.SafeConfigParser()
    cp.read('conf/app.conf')
    db = DB(cp.items('db'))
    main(db)
    endtime = time.time()
    logger.info("end, cost time [%d]\n########", endtime - starttime)

    