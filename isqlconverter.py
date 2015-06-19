'''
process_select_isql returns seconds,results (results are ordered)
process_ask_isql returns seconds,boolean
'''
import string
def process_select_isql(input):
    #print "INPUT***"
    #print input
    results=[]
    lines=string.split(input, '\r\n')
    #print lines
    for line in lines[:-3]:
        results.append(tuple(line.split()))
    t= int(lines[-2].split()[-2])*0.001
    return t,results

def process_ask_isql(input):
    results=[]
    lines=string.split(input, '\r\n')
    #print lines
    if lines[0]=='':
        result=False
        t= int(lines[1].split()[-2])*0.001
    elif int(lines[0])==1:
        result=True
        t= int(lines[2].split()[-2])*0.001
    else:
        print lines
        raise AssertionError
    return t,result