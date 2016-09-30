from __future__ import print_function
import base64
import json
import urllib
import time
import socket
from couchbase.bucket import Bucket, NotFoundError
from couchbase.admin import Admin
from lib.membase.api import httplib2
from lib.membase.api.exception import ServerUnavailableException
import logging.config
import threading
import operator


logging.basicConfig()
log = logging.getLogger()

src_ip = "10.141.150.101"
dst_ip = "10.141.150.102"
src_ip_1 = "10.141.150.103"
dst_ip_2 = "10.141.150.104"
src_port = "8091"
dst_port = "8091"
src_port1 = "8091"
dst_port1 = "8091"


class LWWTtest(object):
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.baseUrl = "http://" + self.ip + ":" + self.port

    def bucket_create(self, bucketname, bucket_type):
        admin = Admin('Administrator', 'password', host=self.ip, port=8091)

        if bucket_type.lower() == 'non-lww':
            admin.bucket_create(name=bucketname, ram_quota=100)
            admin.wait_ready(bucketname, timeout=15.0)
        else:
            self.__bucket_lww('Administrator', 'password', bucketname, ram_quota=100,
                              time_synchronization="enabledWithoutDrift", proxy_port=11217)
            admin.wait_ready(bucketname, timeout=15.0)

    def document_create(self, bucketname, docs=1000):
        cb = Bucket('couchbase://' + self.ip + '/' + bucketname, password='')
        for i in range(1, docs + 1):
            cb.insert(str(i), str(i))
        return cb

    def add_remote_cluster(self, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate=''):
        # example : password:password username:Administrator ip:127.0.0.1:9002 name:two
        msg = "adding remote cluster ip:{0}:{1} with username:password {2}:{3} name:{4} to source node: {5}:{6}"
        log.info(msg.format(remoteIp, remotePort, username, password, name, self.ip, self.port))
        api = self.baseUrl + "/pools/default/remoteClusters"
        return self.__remote_clusters(api, 'add', remoteIp, remotePort, username, password, name, demandEncryption,
                                      certificate)

    def __remote_clusters(self, api, op, remoteIp, remotePort, username, password, name, demandEncryption=0,
                          certificate=''):
        param_map = {'hostname': "{0}:{1}".format(remoteIp, remotePort),
                     'username': username,
                     'password': password,
                     'name': name}
        if demandEncryption:
            param_map['demandEncryption'] = 'on'
            param_map['certificate'] = certificate
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        # sample response :
        # [{"name":"two","uri":"/pools/default/remoteClusters/two","validateURI":"/pools/default/remoteClusters/two?just_validate=1","ip":"127.0.0.1:9002","username":"Administrator"}]
        if status:
            remoteCluster = json.loads(content)
        else:
            log.error("/remoteCluster failed : status:{0},content:{1}".format(status, content))
            raise Exception("remoteCluster API '{0} remote cluster' failed".format(op))
        return remoteCluster

    def __bucket_lww(self, username, password, bucketname, ram_quota, time_synchronization, proxy_port):
        param_map = {'username': username,
                     'password': password,
                     'name': bucketname,
                     'timeSynchronization': time_synchronization,
                     'ramQuotaMB': ram_quota,
                     'proxyPort': proxy_port,
                     'authType': "none"}

        api = self.baseUrl + "/pools/default/buckets"
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        if status:
            log.info("successful")
        else:
            log.error("/pools/default/buckets failed : status:{0},content:{1}".format(status, content))

    def _http_request(self, api, method='GET', params='', headers=None, timeout=120):
        if not headers:
            headers = self._create_headers()
        end_time = time.time() + timeout
        while True:
            try:
                response, content = httplib2.Http(timeout=timeout).request(api, method, params, headers)
                if response['status'] in ['200', '201', '202']:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                    except ValueError as e:
                        json_parsed = {}
                        json_parsed["error"] = "status: {0}, content: {1}".format(response['status'], content)
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    log.error(
                        '{0} error {1} reason: {2} {3}'.format(api, response['status'], reason, content.rstrip('\n')))
                    return False, content, response
            except socket.error as e:
                log.error("socket error while connecting to {0} error {1} ".format(api, e))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            except httplib2.ServerNotFoundError as e:
                log.error("ServerNotFoundError error while connecting to {0} error {1} ".format(api, e))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            time.sleep(3)

    def _create_headers(self):
        authorization = base64.encodestring('%s:%s' % ("Administrator", "password"))
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    def start_replication(self, fromBucket, toCluster, toBucket, rep_type="xmem", xdcr_params={}):
        toBucket = toBucket or fromBucket
        msg = "starting {0} replication type:{1} from {2} to {3} in the remote" \
              " cluster {4} with settings {5}"
        api = self.baseUrl + '/controller/createReplication'
        param_map = {'replicationType': 'continuous',
                     'toBucket': toBucket,
                     'fromBucket': fromBucket,
                     'toCluster': toCluster,
                     'type': rep_type}
        param_map.update(xdcr_params)
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        # response : {"id": "replication_id"}
        if status:
            json_parsed = json.loads(content)
            log.info("Replication created with id: {0}".format(json_parsed['id']))
            replication_id = json_parsed['id'].replace("/", "%2F")
            return replication_id
        else:
            log.error("/controller/createReplication failed : status:{0},content:{1}".format(status, content))
            raise Exception("create replication failed : status:{0},content:{1}".format(status, content))

    def pause_replication(self, replication_id,pauseRequested=True):
        api = self.baseUrl + '/settings/replications/' + replication_id
        param_map = {'pauseRequested': pauseRequested}
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        if status:
            log.info("successful")
        else:
            log.error("/settings/replications/ failed : status:{0},content:{1}".format(status, content))

    def graceful_failover(self, failover_node, wait=0):
        count = 0
        api = self.baseUrl + '/controller/startGracefulFailover'
        param_map = {'otpNode': 'ns_1@' + failover_node}
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        if status:
            log.info("successful")
        else:
            log.error("/controller/startGracefulFailover failed : status:{0},content:{1}".format(status, content))
        if wait:
            api = self.baseUrl + '/nodeStatuses'
            status, content, _ = self._http_request(api, 'GET')
            info = json.loads(content)
            while info[failover_node + ":8091"]["gracefulFailoverPossible"] == True and count != 10:
                log.info("sleeping")
                time.sleep(5)
                count += 1

    def cluster_rebalance(self, failover_node):
        api = self.baseUrl + '/controller/rebalance'
        param_map = {'ejectedNodes': "",
                     'knownNodes': 'ns_1@' + self.ip + ',ns_1@' + failover_node}
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        if status:
            log.info("successful")
        else:
            log.error("/controller/rebalance failed : status:{0},content:{1}".format(status, content))

    def _item_count(self, bucketname):
        api = self.baseUrl + '/pools/default/buckets/' + bucketname
        status, content, _ = self._http_request(api, 'GET')
        if status:
            log.info("successful")
        else:
            log.error("/pools/default/buckets/ failed : status:{0},content:{1}".format(status, content))
        json_parsed = json.loads(content)
        return json_parsed['basicStats']['itemCount']

    def mutations(self, bucketname):
        item_count = self._item_count(bucketname)
        cb = Bucket('couchbase://' + self.ip + '/' + bucketname, password='')
        for i in range(1, item_count, 4):
            result = cb.get(str(i))
            cb.upsert(str(i), result.value + bucketname + "'")
            result = cb.get(str(i+1))
            cb.replace(str(i + 1), result.value + bucketname + "'")
            cb.remove(str(i + 2))

        for i in range(item_count+1, item_count+item_count/4 + 1):
            cb.insert(str(i), str(i))

    def comparison(self, src_ip, src_bucketname,compare, dst_ip, dst_bucketname,docs=10000):
        mappings = { '<': operator.lt, '<=': operator.le,
                     '>': operator.gt, '>=': operator.ge,
                     '==': operator.eq, '!=': operator.ne }
        cb1 = Bucket('couchbase://' + src_ip + '/' + src_bucketname, password='')
        cb2 = Bucket('couchbase://' + dst_ip + '/' + dst_bucketname, password='')
        for i in range(1, docs + docs/4 + 1):
            key = str(i)
            try:
                value_src = cb1.get(key)
                value_dst = cb2.get(key)
                if not mappings[compare](value_src.cas, value_dst.cas):
                    print (key + " :  " + str(value_src.cas) + ">" + str(value_dst.cas))
                    return False
                else:
                    print (key + " :  " + str(value_src.cas) + "<=" + str(value_dst.cas))
            except NotFoundError:
                print ("key missing : " + key)

        return True



class Test:
    lww1 = LWWTtest(src_ip, src_port)
    lww2 = LWWTtest(dst_ip, dst_port)

    lww1.bucket_create("src", "lww")
    cb1 = lww1.document_create("src", docs=100000)

    lww2.bucket_create("dst", "lww")
    cb2 = lww2.document_create("dst", docs=100000)

    op1 = lww1.add_remote_cluster(dst_ip, dst_port, "Administrator", "password", "AB")
    op2 = lww2.add_remote_cluster(src_ip, src_port, "Administrator", "password", "BA")

    rep1 = lww1.start_replication("src", "AB", "dst")
    rep2 = lww2.start_replication("dst", "BA", "src")

    lww1.pause_replication(rep1)
    lww2.pause_replication(rep2)

    # lww1.graceful_failover(src_ip_1, wait=1)
    # lww1.cluster_rebalance(src_ip_1)
    time.sleep(30)
    #
    # t1 = threading.Thread(target=lww1.mutations, args=("src",))
    # t2 = threading.Thread(target=lww2.mutations, args=("dst",))
    # t1.start()
    # t2.start()
    #
    # t1.join()
    # t2.join()

    lww1.mutations("src")
    lww2.mutations("dst")

    # lww1.pause_replication(rep1, pauseRequested=False)
    lww2.pause_replication(rep2, pauseRequested=False)

    time.sleep(60)

    value = lww1.comparison(src_ip,"src","<=",dst_ip,"dst", docs=100000)
    if value:
        print ("passed")
    else:
        print ("failed")


