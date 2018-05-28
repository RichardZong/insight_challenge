#!/usr/bin/python3
# Packages required for the applicaiton
from operator import itemgetter
from itertools import groupby
import pandas as pd
import toolz
import datetime

inactivity_file=open('input/inactivity_period.txt')
inactivity_period=int(inactivity_file.readline())
inactivity_file.close()

def withdraw_record():
    # Withdraw the file to be processed
    file=open('input/log.csv')
    file_head=file.readline().rstrip().split(",")
    # Find the head and location index of data columns
    ip_location=file_head.index('ip')
    date_location=file_head.index('date')
    time_location=file_head.index('time')
    cik_location=file_head.index('cik')
    accession_location=file_head.index('accession')
    extention_location=file_head.index('extention')
    # Get the file and columns index
    return file,ip_location,date_location,time_location,cik_location,accession_location,extention_location
    file.close()

withdraw_result=withdraw_record()

def individual_record(file, *locations):
    # Extract the information for each individual ip
    file_record=[]
    # Extract informaton from the second line
    for i,line in enumerate(file.readlines()):
        line=line.rstrip().split(",")
        file_record.append([line[ip_location],line[date_location],line[time_location],
                           line[cik_location],line[accession_location],line[extention_location]])
    file_record=sorted(file_record,key=itemgetter(0))
    # Extract all ip using the service
    ip=[]
    for i in range(len(file_record)):
        ip.append(file_record[i][0])
    record={}
    for key,item in groupby(file_record,key=itemgetter(0)):
        record.update({key:list(item)})
    # Same ip could use the service multiple times
    unique_ip=set(ip)
    # Each unique ip will have a dataframe to contain its information
    individual_record_df_generated=[]
    for i in unique_ip:
        individual_collected=toolz.itertoolz.get(i,record)
        individual_record_df=pd.DataFrame(individual_collected,
                                      columns=['ip','date','time','cik','accession','extention'])

        individual_record_df_generated.append(individual_record_df)


    return individual_record_df_generated

file=withdraw_result[0]
ip_location=withdraw_result[1]
date_location=withdraw_result[2]
time_location=withdraw_result[3]
cik_location=withdraw_result[4]
accession_location=withdraw_result[5]
extention_location=withdraw_result[6]
individual_output=individual_record(file,ip_location,date_location,time_location,cik_location,accession_location,extention_location)

def sessions_in_individual_ip(df):# get all sessions for each individual_ip
    # adding the date and time together,generating the exact loging time for an individual_ip
    df['new_time']=pd.Series(df.date+" "+df.time)
    # one individual_ip may have different sessions. The session will be considered as a new
    # session if the same ip log in after the inactivity period and relog in
    s=[]
    for i in range(0,len(df)-1):
        if pd.Timestamp(df.new_time.iloc[i+1])-pd.Timestamp(df.new_time.iloc[i])>pd.Timedelta(inactivity_period,unit='s'):
            #if the different of adjacent two log-in time exceeds specific seconds, the second log-in will be
            # consided as the begining of new session.
            # This step is finding the index of new session in the individual dataframe
            s.append(i+1)
    result=[]
    if len(s)==0:
            df0=df
            request_number=len(df0.accession.unique())
            # A individual_ip log-in will last at least one second
            duration=pd.Timestamp(df0.new_time.iloc[-1])-pd.Timestamp(df0.new_time.iloc[0])+datetime.timedelta(seconds=1)
            result.append([df0.ip.iloc[0],df0.new_time.iloc[0],df0.new_time.iloc[-1],duration.seconds,request_number])
        # print all sessions for invidual_ip
    else:
        for i in range(len(s)):
        # if the sessions is empty, means there is no new session in this individual_ip

        # print all sessions for invidual_ip
            if i==0:
                df1=df[:s[i]]
                request_number=len(df1.accession.unique())
                duration=pd.Timestamp(df1.new_time.iloc[-1])-pd.Timestamp(df1.new_time.iloc[0])+datetime.timedelta(seconds=1)
                result.append([df1.ip.iloc[0],df1.new_time.iloc[0],df1.new_time.iloc[-1],duration.seconds,request_number])
            if i==len(s)-1:
                df2=df[s[i]:]
                request_number=len(df2.accession.unique())
                duration=pd.Timestamp(df2.new_time.iloc[-1])-pd.Timestamp(df2.new_time.iloc[0])+datetime.timedelta(seconds=1)
                result.append([df2.ip.iloc[0],df2.new_time.iloc[0],df2.new_time.iloc[-1],duration.seconds,request_number])
            else:
                df3=df[s[i-1]:s[i]]
                request_number=len(df3.accession.unique())
                duration=pd.Timestamp(df3.new_time.iloc[-1])-pd.Timestamp(df3.new_time.iloc[0])+datetime.timedelta(seconds=1)
                result.append([df3.ip.iloc[0],df3.new_time.iloc[0],df3.new_time.iloc[-1],duration.seconds,request_number])
    return result

final_result=[]
for i in range(len(individual_output)):
    #print the record with this order (ip,date and time the first webpage request,date and time the last
    # webpage request,duration,request number)
    final_result.append(sessions_in_individual_ip(individual_output[i]))
final=[]
for i in sorted(final_result,key= itemgetter(0)):
    for j in i:
        final.append((",".join(map(str,j))))
        print(",".join(map(str,j)))
with open('output/sessionization.txt','a+') as f:

    for i in final:
        f.writelines('%s\n' % i)