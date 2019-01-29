from cassandra.cluster import Cluster
import sys


toolbar_width = 10

print ""
# setup toolbar
sys.stdout.write("CR4:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['




cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

districts = session.execute('SELECT d_id  FROM district')
i = 0
result = [True] * 100
for district in districts:
    d_id=district.d_id
    #print ""
    #print "district #" + str(d_id)
    
    orderlines = session.execute('select count(*) from ORDER_LINE where OL_D_ID=' + str(d_id) + ' ALLOW FILTERING')
    for orderline in orderlines:
        count_order_line = orderline[0]
    #print "count_order_line =" + str(count_order_line)
    
    orders = session.execute('select sum(O_OL_CNT) from OORDER where O_D_ID=' + str(d_id) + ' ALLOW FILTERING')
    for order in orders:
        sum_o_ol_cnt = order[0]
    #print "sum_o_ol_cnt     =" + str(sum_o_ol_cnt)
    result[i] = (sum_o_ol_cnt==count_order_line)
    #print  result[i]
    i = i+1
    sys.stdout.write("-")
    sys.stdout.flush()
            
sys.stdout.write("\n")
            
if all(result): 
    print ">> preserved"
else:
    print ">> violated"



