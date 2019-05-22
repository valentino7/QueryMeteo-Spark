import sys, re,json,requests,time




def send_request(url):
    request = requests.get(url)
    page = request.json()
    return page

def run_input(ip):
    url = 'http://'+ip+':9999/nifi-api/flow/search-results?q=a66b462f-82ae-3329'
    page=send_request(url)
    #cerco l'id del service
    properties=page["searchResultsDTO"]["processorResults"][0]["matches"]
    id_service=str(properties).split()[4]
    id_service= id_service[:len(id_service)-2]
      
    #setto enabled le stringhe di run
    url_service = 'http://'+ip+':9999/nifi-api/controller-services/'+id_service
    page=send_request(url_service)
    
    page["component"]["state"]="ENABLED"
    page["status"]["runStatus"]="ENABLED"
    requests.put(url_service, json=page)
    
    #cerco l'id del processorGroup
    url_search_id_processorGroup= 'http://'+ip+':9999/nifi-api/flow/search-results?q=PutcsvToHDFSinParquet'
    page=send_request(url_search_id_processorGroup)
    group_id=page["searchResultsDTO"]["processGroupResults"][0]["id"]
    
    time.sleep(5)
    
    #run
    url_run='http://'+ip+':9999/nifi-api/flow/process-groups/'+group_id
    json_run={
            "id":group_id,"state":"RUNNING"
            }
    print json_run
    requests.put(url_run, json=json_run)
    print True
    return True

def output(ip):
    print ip
    url = 'http://'+ip+':9999/nifi-api/flow/search-results?q=a96673f5-bffe-32f8'
    print url
    page=send_request(url)
    #cerco l'id del service
    properties=page["searchResultsDTO"]["processorResults"][0]["matches"]
    id_service=str(properties).split()[6]
    id_service= id_service[:len(id_service)-2]
    
     #setto enabled le stringhe di run
    url_service = 'http://'+ip+':9999/nifi-api/controller-services/'+id_service
    page=send_request(url_service)
    
    page["component"]["state"]="ENABLED"
    page["status"]["runStatus"]="ENABLED"
    requests.put(url_service, json=page)
    
     #cerco l'id del processorGroup
    url_search_id_processorGroup= 'http://'+ip+':9999/nifi-api/flow/search-results?q=exportHDFSToDbs'
    page=send_request(url_search_id_processorGroup)
    group_id=page["searchResultsDTO"]["processGroupResults"][0]["id"]
    
    print url_service
    
    #time.sleep(5)
    
    #run
    url_run='http://'+ip+':9999/nifi-api/flow/process-groups/'+group_id
    json_run={
            "id":group_id,"state":"RUNNING"
            }
    print json_run
    requests.put(url_run, json=json_run)
    

    print True
    return True



    

if __name__ == '__main__':
    if sys.argv[1]=="1":
        run_input(sys.argv[2])
    else:
        output(sys.argv[2])


