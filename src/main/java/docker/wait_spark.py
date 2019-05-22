import sys, re,json,requests,time,re

def send_request(url):
    request = requests.get(url)
    return request

def wait_spark(ip):
    url = 'http://'+str(ip)
    while(True):
        page=send_request(url)
        s = '<td>FINISHED</td>'
        result = re.findall(s, page.text)
        if len(result)==3:
            break
        time.sleep(10)
        
    print result
    return True



    

if __name__ == '__main__':
    wait_spark(sys.argv[1])



 
