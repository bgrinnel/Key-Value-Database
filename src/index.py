from flask import Flask
import requests
from flask import request
import os
import sys
import random


# TODO fix get data function
# TODO QR

app = Flask(__name__)
PORT = 8080

INITIALIZED = False
ADDRESS = os.environ.get('ADDRESS')

# Maximum permitted value size
MAX_VAL_SIZE = 8*1000000    # 8MB

# GLOBAL VARIABLES =======================================================================================
# dictionary of keys and values being stored
kvdict = {}
# all node addresses in cluster in order for chain replication
View = []       #chain in order
#Uses addresses as keys, node clock is value
dependency_list = []
# =======================================================================================

# ADMIN ====================================================================================================

#kvs/admin/view/altView --------------------------------------------------------------------------------
@app.route("/kvs/admin/view/altView", methods=['PUT'])
def admin_altView_put():
    global View    

    # Grab all data from put request
    data = request.get_json()
    # From data, extract view list
    try:
        newView = data['view']
    except:
        raise IndexError("view was not found in request")
    
    # clear the view completely
    View = newView

    # initialize
    global INITIALIZED
    INITIALIZED = True

    return  {}, 200

#kvs/admin/view ----------------------------------------------------------------------------------------
@app.route("/kvs/admin/view", methods=['GET'])
def admin_view_get():

    return {"view": View}, 200

# ------------------------------------------------------------------------------------------------------
@app.route("/kvs/admin/view", methods=['PUT'])
def admin_view_put():
    # Global variables
    global View
    global kvdict

    # Initialized since we are getting a view
    global INITIALIZED
    INITIALIZED = True

    # Grab all data from put request
    data = request.get_json()
    newView = data['view']

    # get the set of nodes to add (nodes not in our view)
    add_nodes = set(newView) - set(View)
    # get the set of nodes to delete (nodes not in the client's view)
    del_nodes = set(View) - set(newView)
    recurringNodes = set(newView) - set(add_nodes)
    # for all nodes that are shared, overwrite their view with this new view

    for x in recurringNodes:

        # do not act on our own address
        if x == ADDRESS:
            continue
        # format data to forward
        #data = {"view":newView}
        put_url = 'http://{}/kvs/admin/view/altView'.format(x)
        requests.put(put_url, json = request.get_json(), timeout = 10)

    for x in add_nodes:

        # skip our own address
        if x == ADDRESS:
            continue
        
        # overwrite view with a put to /altView
        put_url = 'http://{}/kvs/admin/view/altView'.format(x)
        requests.put(put_url, json = request.get_json(),timeout = 10)

        # replicate our kvs store to them
        for key in kvdict:
            # x = a node we're adding to the view
            # key = a key in our kvs
            put_url = 'http://{}/kvs/data/{}'.format(x, key)
            # send put requests and inform them that we are a node, not a client
            # put data is included ("val" : <key value>)
            # custom metadata informing that we are a node
            metadata = {"val" : kvdict[key], "source" : "replica"}
            requests.put(put_url, json = metadata)

    # for all nodes not in the client's view but in our view (nodes to delete)
    for x in del_nodes:
        # skip our own address
        if x == ADDRESS:
            continue
        # delete from view
        del_url = 'http://{}/admin/view'.format(x)
        requests.delete(del_url)
    
    # set our view to new view
    View = newView

    return {}, 200

# ------------------------------------------------------------------------------------------------------
@app.route("/kvs/admin/view", methods=['DELETE'])
def admin_view_delete():
    global View
    global dependency_list
    global kvdict
    global INITIALIZED

    # Uninitialize ourself
    INITIALIZED = False

    # delete our view
    View = []
    # clear our dependency list
    dependency_list.clear()
    # clear our kvs
    kvdict.clear()
    
    return {}, 200

#====================================================================================================

# Key-Value-Store =================================================================================
# GET ---------------------------------------------
#kvs/data/<Key>
@app.route("/kvs/data/<key>", methods=['GET'])
def kvs_get(key):

    # only continue if we are part of a cluster
    if(INITIALIZED):
        global dependency_list
         # grab our dependency list
        metadata = dependency_list
        # check that value exists first
        if (key not in kvdict) :
            return {"causal-metadata": metadata}, 404
        # return the value gotten (if it exists)
        return  {"val": kvdict[key], "causal-metadata": metadata}, 200
    else:
        return {"error": "uninitialized"}, 418

# PUT ---------------------------------------------
@app.route("/kvs/data/<key>", methods=['PUT'])
def kvs_put(key):

    # only continue if the node is initialized
    if(INITIALIZED):
        global dependency_list
        request_data = request.get_json()

        # Code up until "if "source" in request_data:" not integrated

        # N = number of nodes in view
        N = len(View)
        # if N is even, divide normally
        if(N % 2 == 0):
            W = N//2
        # if N is odd, go up by one to fulfill W > N/2
        else:
            W = N//2 + 1
        # subtract 1 write to account for our own write
        W = W - 1

        # for ourselves (node receiving the put from the client)
        # process one write ourselves as normal

        # pick nodes W times if W > 1
        # if W <= 1, then theres only at most ourselves who can process the request, so do not forward
        if(W >= 1):
            # forward the write/put to W > N/2 other nodes randomly in our view
            toForward = set()
            for _ in range(W):
                # pick a random node in our view that is not ourselves and is not already one of the nodes picked
                next = random.choice([node for node in View if node != ADDRESS and node not in toForward])
                toForward.add(next)
        # for each node in toForward, do the write
        
        # fulfilling request from source node
        if "source" in request_data:
            val = request_data['val']
            #standard update
            new_list = request_data['causal-metadata']
            # if dep list is missing operations
            if len(new_list) > len(dependency_list):
                for o in new_list:
                    if o not in dependency_list:
                        if 'put' in o:
                            eq = o.find('=')
                            key = o[3:eq]
                            val = o[eq+1:]
                            kvdict[key]= val
                        elif 'del' in o:
                            key = o[3:]
                            del kvdict[key]
                        dependency_list.append(o)
            # if dep list has more operations
            elif len(new_list) < len(dependency_list):
                temp_list = []
                for o in dependency_list:
                    if o not in new_list:
                        temp_list.append(o)
                        dependency_list.remove(o)
                    elif o not in dependency_list:
                        if 'put' in o:
                            eq = o.find('=')
                            key = o[3:eq]
                            val = o[eq+1:]
                            kvdict[key]= val
                        elif 'del' in o:
                            key = o[3:]
                            del kvdict[key]
                        dependency_list.append(o)
                for i in temp_list:
                    if 'put' in o:
                        eq = o.find('=')
                        key = o[3:eq]
                        val = o[eq+1:]
                        kvdict[key]= val
                    elif 'del' in o:
                        key = o[3:]
                        del kvdict[key]
                    dependency_list.append(i)
            return{"causal-metadata": dependency_list}, 200

        # fulfilling request as source node
        else:
            # if 'val' in data to be put
            if 'val' in request_data:
                # save val from put data
                val = request_data['val']
                #save metadata from request
                request_data["causal-metadata"]
                # if key or val not valid, produce error
                if key == None or val == None:
                    return {"error": "bad PUT"}, 400
                # if val too large, produce error
                elif sys.getsizeof(val) > MAX_VAL_SIZE:
                    return {"error": "val too large"}, 400
                # if key already exists
                elif key in kvdict:
                    # replace key value with new val
                    kvdict[key] = val
                    str = 'put{}={}'.format(key,val)
                    dependency_list.append(str)                    
                    
                    resend = True
                    # loop until all nodes agree on metadata
                    while(resend):
                        metadata = {'val': val, "source" : "replica", 'causal-metadata': dependency_list}
                        resend = False
                        # for all nodes we know about
                        for x in View:
                            # do not process ourself
                            if x == ADDRESS:
                                continue
                            # url to use for putting data to this node
                            put_url = 'http://{}/kvs/data/{}'.format(x,key)
                            # put the data in other node
                            response = requests.put(put_url,json = metadata,verify=False, timeout=10)
                            # make sure the data matches our version
                            resData = response.json()
                            if dependency_list != resData['causal-metadata']:
                                if len(dependency_list) < resData['causal-metadata']:
                                    dependency_list = resData['causal-metadata']
                                #resend = True                    

                    return{"causal-metadata": dependency_list}, 200
                # if key does not already exist
                else:
                    # set new key value as new val
                    kvdict[key] = val   
                    str = 'put{}={}'.format(key,val)
                    dependency_list.append(str)                    
                    
                    resend = True
                    # loop until all nodes agree on metadata
                    while(resend):
                        metadata = {'val': val, "source" : "replica", 'causal-metadata': dependency_list}
                        resend = False
                        # for all nodes we know about
                        for x in View:
                            # do not process ourself
                            if x == ADDRESS:
                                continue
                            # url to use for putting data to this node
                            put_url = 'http://{}/kvs/data/{}'.format(x,key)
                            # put the data in other node
                            response = requests.put(put_url,json = metadata,verify=False, timeout=10)
                            # make sure the data matches our version
                            resData = response.json()
                            if dependency_list != resData['causal-metadata']:
                                if len(dependency_list) < resData['causal-metadata']:
                                    dependency_list = resData['causal-metadata']
                                #resend = True

                    return {"causal-metadata": dependency_list}, 201
            # if get does not have a 'val' in data, produce error
            else:
                return {"error": "bad PUT"}, 400
    # if uninitialied, produce uninitialized error
    else:
        return {"error": "uninitialized"}, 418


# DELETE ---------------------------------------------
@app.route("/kvs/data/<key>", methods=['DELETE'])
def kvs_delete(key):
    if(INITIALIZED):
        global View
        global dependency_list


        # a client will not send request data with a delete request
        # but a node will send meta data with "source" to indicate that the delete is coming from a node
        request_data = request.get_json()
        # indicate to other nodes that we are updating them, not sending an original request

        # if node is not origin of 
        if "source" in request_data :
            new_list = request_data['causal-metadata']
            if len(new_list) > len(dependency_list):
                for o in new_list:
                    if o not in dependency_list:
                        if 'put' in o:
                            eq = o.find('=')
                            key = o[3:eq]
                            val = o[eq+1:]
                            kvdict[key]= val
                        elif 'del' in o:
                            key = o[3:]
                            del kvdict[key]
                        dependency_list.append(o)
            elif len(new_list) < len(dependency_list):
                temp_list = []
                for o in dependency_list:
                    if o not in new_list:
                        temp_list.append(o)
                        dependency_list.remove(o)
                    elif o not in dependency_list:
                        if 'put' in o:
                            eq = o.find('=')
                            key = o[3:eq]
                            val = o[eq+1:]
                            kvdict[key]= val
                        elif 'del' in o:
                            key = o[3:]
                            del kvdict[key]
                        dependency_list.append(o)
                for i in temp_list:
                    if 'put' in o:
                        eq = o.find('=')
                        key = o[3:eq]
                        val = o[eq+1:]
                        kvdict[key]= val
                    elif 'del' in o:
                        key = o[3:]
                        del kvdict[key]
                    dependency_list.append(i)
            return{"causal-metadata": dependency_list}, 200
        
        if key in kvdict:
            del kvdict[key]
            str = 'del{}'.format(key)
            dependency_list.append(str)
            resend = True
            # loop until all nodes agree on metadata
            while(resend):
                metadata = {"source" : "replica", 'causal-metadata': dependency_list}
                resend = False
                # for all nodes we know about
                for x in View:
                    # do not process ourself
                    if x == ADDRESS:
                        continue
                    # url to use for putting data to this node
                    del_url = 'http://{}/kvs/data/{}'.format(x,key)
                    # put the data in other node
                    response = requests.delete(del_url,json = metadata,verify=False, timeout=10)
                    # make sure the data matches our version
                    resData = response.json()
                    if dependency_list != resData['causal-metadata']:
                        if len(dependency_list) < resData['causal-metadata']:
                            dependency_list = resData['causal-metadata']
                        #resend = True

            return  {"causal-metadata": metadata}, 200
        else:
            return {"error": "not found"}, 404
    else:
        return {"error": "uninitialized"}, 418
    
# ENTIRE DATA SET 
#kvs/data
@app.route("/kvs/data", methods=['GET'])
def kvs_data():
    global dependency_list
    request_data = request.get_json()
    metadata = {"source" : "replica"}
    # only reach out to other nodes if we were contacted by the client
    
    return {"count": len(kvdict), "keys": list(kvdict.keys()),"causal-metadata": dependency_list}, 200

#====================================================================================================


if __name__ == "__main__":
    app.run(port = 8080, host = "0.0.0.0")