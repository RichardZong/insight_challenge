Summary of this challenge
In this challenge, the ip can be the key to identify different users. One ip could have multiple times to visit the webpages.Based on the chanllenge requirement, a single user session is defined to have started when the IP address first requests a document from the EDGAR system and continues as long as the same user continues to make requests. The session is over after a certain period of time has elapsed. Therefore, one unique ip address could have multiple sessions depending on the adjacent request time. For example, if the different of two adjacent request time for one ip exceeds the inactivity period, the second time can be considered as the new session begining time.

Considering the data size is huge, it is hard to extract all information in one dataframe. However, the information for one particular ip may not oversize. With respect to this, the extracting only information from one unique ip address could be feasible for Pandas to handle. Each pandas dataframe contains only information of one ip address.

The required packages in this design are Operator,toolz,itertools,pandas,and datetime



implementation details
two input files

log.csv downloaded from EDGAR website, all data in one month was downloaded
inactivity_period.txt, it is self-assigned value, this design assigned 10 seconds

output files

IP address of the user exactly as found in log.csv
date and time of the first webpage request in the session (yyyy-mm-dd hh:mm:ss)
date and time of the last webpage request in the session (yyyy-mm-dd hh:mm:ss)
duration of the session in seconds
count of webpage requests during the session

Tests

There are two tests in the test fold. Test 1 contains the input files from insight
data source. Test2 was created from downloaded log.csv, which contains one month
data. 100 rows of data has been extracted from the log.csv as the test 2.

Finally, considering the file size of original downloaded log.csv to Github, the submitted log.csv is the first 1000 rows extracted from the original log.csv
