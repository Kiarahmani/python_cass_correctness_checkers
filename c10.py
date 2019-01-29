from cassandra.cluster import Cluster
import time
import sys
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-q", "--quiet",
                    action="store_false", dest="verbose", default=True,
                    help="don't print status messages to stdout")
args = parser.parse_args()

def my_print(msg):
    if args.verbose:
        print msg

toolbar_width = 10

print ""
# setup toolbar
sys.stdout.write("CR10:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['


cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

i = 0
result = [True] * 100

districts = session.execute('SELECT d_id  FROM district')
for district in districts:
    d_id=district.d_id
    customers = session.execute('select c_id,c_balance  FROM customer where c_w_id=1 and c_d_id='+ str(d_id))
    for customer in customers:
        my_print ("")
        c_id=customer.c_id
        my_print ("c_id:"+str(c_id))
        c_balance=customer.c_balance
        my_print ("c_balanec:"+str(c_balance))
        expected_total = c_balance
        
        orders = session.execute('select o_id from oorder  WHERE o_w_id=1 and o_d_id='+str(d_id)+' and o_c_id='+str(c_id) + ' ALLOW FILTERING')
        ol_total=0;
        for order in orders:
            o_id=order.o_id
            my_print (" o_id:"+str(o_id))
            order_lines = session.execute('select ol_amount,ol_delivery_d from order_line WHERE ol_w_id=1 and ol_d_id='+str(d_id)+' and ol_o_id='+str(o_id))
            for order_line in order_lines:
                my_print ("     ol_delivery_d:"+str(order_line.ol_delivery_d)+", "+str(order_line.ol_amount))
                if order_line.ol_delivery_d!=None:
                    ol_total=ol_total+order_line.ol_amount
            my_print (" ol_total:"+str(ol_total))

        histories = session.execute('select h_amount from history where h_c_w_id=1 and h_c_d_id='+ str(d_id) +' and h_c_id='+str(c_id))
        h_total=0
        for history in histories:
            h_amount = history.h_amount
            my_print("     h_amount:"+str(h_amount))
            h_total = h_total + h_amount
        my_print(" h_total:"+str(h_total))
        if expected_total!=(ol_total-h_total):
            result[i]=False
            #break
    my_print (str(result[i]))
    if result[i]==False:
        break;
    sys.stdout.write("-")
    sys.stdout.flush()
    i=i+1
sys.stdout.write("\n")

if all(result):
    print ">> preserved"
else:
    print ">> violated"


