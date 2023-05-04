import datetime
f = open("matchcount.txt", "w")
f.write(f'{16} MATCHES PLAYED AS OF {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %p")}')