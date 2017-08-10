# summer-work-experience
backfill.py uses a Command Line Interface (CLI) to check active dropbox licenses (stored in a database) and parse each value of each record. The periodic result of the checking process is stored in a central repository using RESTFUL API. A license is a the permission given to a user to use Dropbox, each license is stored in a csv file with the date they were first bought (format: YYYY-MM-DD), the owner, and whether the license is active or not. The "start date" is then converted into how many nanoseconds have passed from the 1st of january 1970 until the start date of the license. 

The CLI is used as follows:

The user has to give some required arguments and other optional arguments, these are: the file were the licenses are stored; whether the user wants to parse the licenses up to a certain  date; the time series; the database where the results are going to be posted to; credentials to access this database (username and password); how quicly the monitoring system takes this measurements; the url of the server; whether the user wants to apply the dry run or not.

My pathway to an optimised code can be seen in the different branches of this project.
