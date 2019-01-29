import sys
from cassandra.cluster import Cluster
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
sys.stdout.write("NCR1:[%s]" % (" " * toolbar_width))
sys.stdout.flush()
sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['

cluster = Cluster(["n2"])
session = cluster.connect("tpcc")

i = 0

#######################################################################
# get the number of times the main loop will happen
stocks = session.execute('SELECT count(*) FROM stock where s_ytd>0 ALLOW FILTERING')
for stock in stocks:
    st_len=stock[0]
result = [True] * st_len

# main loop
stocks = session.execute('SELECT s_i_id,s_ytd FROM stock where s_ytd>0 ALLOW FILTERING')
for stock in stocks:
    my_print("")
    s_i_id = stock.s_i_id
    s_ytd  = stock.s_ytd
    my_print("s_i_id: "+str(s_i_id))
    my_print("s_ytd:    "+str(s_ytd))
    # retrieve order_lines
    order_lines = session.execute('select ol_quantity from order_line WHERE ol_w_id=1 and ol_i_id='+str(s_i_id)+' ALLOW FILTERING')
    ol_total=0
    for order_line in order_lines:
        ol_quantity = order_line.ol_quantity
        if ol_quantity!=5:
            ol_total = ol_total+ol_quantity

    my_print("ol_total: "+str(ol_total))
    result[i] = (ol_total==s_ytd)
    if (result[i]==False and  ol_total<s_ytd):
        print "--"
        print "s_i_id: "+str(s_i_id)
        print "s_ytd:    "+str(s_ytd)
        print "ol_total: "+str(ol_total)
        print ""
        
    my_print(str(result[i]))
    i = i+1
    if i%(st_len/10)==0:
        sys.stdout.write("-")
        sys.stdout.flush()
            
sys.stdout.write("\n")
            
if all(result): 
    print ">> preserved"
else:
    print ">> violated"

