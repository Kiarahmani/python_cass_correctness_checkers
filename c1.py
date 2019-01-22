from cassandra.cluster import Cluster
import sys

toolbar_width = 1
print ""
# setup toolbar
sys.stdout.write("CR1:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['
cluster = Cluster(["n2"])
session = cluster.connect("tpcc")
rows = session.execute('SELECT w_ytd FROM warehouse')
for user_row in rows:
    w_ytd=user_row.w_ytd
rows = session.execute('SELECT SUM(d_ytd) FROM district')
for user_row in rows:
    sd_ytd=user_row[0]
#print "sum(D_YTD) =",sd_ytd
#print "W_YTD      =",w_ytd

sys.stdout.write("-")
sys.stdout.flush()
            
sys.stdout.write("\n")
            
if sd_ytd==w_ytd: 
    print ">> preserved"
else:
    print ">> violated"
        


