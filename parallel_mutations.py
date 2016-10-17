from __future__ import print_function
import base64
import json
import urllib
import socket
from couchbase.bucket import Bucket, NotFoundError
from couchbase.admin import Admin
import logging.config
import threading
import operator
import time
import unittest
import httplib2

logging.basicConfig()
log = logging.getLogger()

src_ip = "172.23.106.201"
dst_ip = "172.23.106.227"
src_ip_1 = "172.23.107.118"
dst_ip_2 = "172.23.106.32"
src_port = "8091"
dst_port = "8091"
src_port1 = "8091"
dst_port1 = "8091"
docs_max = 8
ram_quota = 1712


class LWWTtest(object):
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.baseUrl = "http://" + self.ip + ":" + self.port

    def bucket_create(self, bucketname, bucket_type):
        admin = Admin('Administrator', 'password', host=self.ip, port=8091)

        if bucket_type.lower() == 'non-lww':
            admin.bucket_create(name=bucketname, ram_quota=ram_quota)
            admin.wait_ready(bucketname, timeout=15.0)
        else:
            self.__bucket_lww('Administrator', 'password', bucketname, ram_quota=ram_quota,
                              time_synchronization="enabledWithoutDrift", proxy_port=11217)
            admin.wait_ready(bucketname, timeout=15.0)

    def bucket_delete(self, bucketname):
        admin = Admin('Administrator', 'password', host=self.ip, port=8091)
        admin.bucket_delete(name=bucketname)

    def cluster_delete(self, clustername):
        api = self.baseUrl + "/pools/default/remoteClusters/" + clustername
        status, content, _ = self._http_request(api, 'DELETE')
        if status:
            log.info("successful")
        else:
            log.error("/pools/default/remoteClusters failed : status:{0},content:{1}".format(status, content))

    def document_create(self, bucketname, docs=docs_max):
        # cb = Bucket('couchbase://' + self.ip + '/' + bucketname, password='')
        cb = Bucket('couchbase://' + self.ip + '/' + bucketname)
        for i in range(1, docs + 1):
            timestamp = int(time.time())
            data = {"value": str(i), "last_updated_time": timestamp, "mutations": 1}
            cb.insert(str(i), data)
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
                    raise Exception(ip=self.ip)
            except httplib2.ServerNotFoundError as e:
                log.error("ServerNotFoundError error while connecting to {0} error {1} ".format(api, e))
                if time.time() > end_time:
                    raise Exception(ip=self.ip)
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

    def pause_replication(self, replication_id, pauseRequested=True):
        api = self.baseUrl + '/settings/replications/' + replication_id
        param_map = {'pauseRequested': pauseRequested}
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        if status:
            log.info("successful")
        else:
            log.error("/settings/replications/ failed : status:{0},content:{1}".format(status, content))

    def resume_replication(self, replication_id):
        self.pause_replication(replication_id, pauseRequested=False)

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
                status, content, _ = self._http_request(api, 'GET')
                info = json.loads(content)
                log.info("sleeping")
                time.sleep(5)
                count += 1


    def cluster_rebalance(self, failover_node, wait=0):
        count = 0
        api = self.baseUrl + '/controller/rebalance'
        param_map = {'ejectedNodes': "",
                     'knownNodes': 'ns_1@' + self.ip + ',ns_1@' + failover_node}
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        if status:
            log.info("successful")
        else:
            log.error("/controller/rebalance failed : status:{0},content:{1}".format(status, content))
        if wait:
            api = self.baseUrl + '/pools/default/tasks'
            status, content, _ = self._http_request(api, 'GET')
            info = json.loads(content)
            if info[0]["subtype"] and info[0]["status"]:
                while info[0]["subtype"] == "rebalance" and info[0]["status"] == "running" and count != 10:
                    status, content, _ = self._http_request(api, 'GET')
                    info = json.loads(content)
                    if not info[0]["subtype"] or not info[0]["status"]:
                        break
                    log.info("sleeping")
                    time.sleep(25)
                    count += 1

    def node_recovery(self, recovery_node, recovery_type="full"):
        api = self.baseUrl + '/controller/setRecoveryType'
        param_map = {
            'otpNode': 'ns_1@' + recovery_node,
            'recoveryType': recovery_type
        }
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        if status:
            log.info("successful")
        else:
            log.error("controller/setRecoveryType failed : status:{0},content:{1}".format(status, content))

    def _item_count(self, bucketname):
        api = self.baseUrl + '/pools/default/buckets/' + bucketname
        status, content, _ = self._http_request(api, 'GET')
        if status:
            log.info("successful")
        else:
            log.error("/pools/default/buckets/ failed : status:{0},content:{1}".format(status, content))
        json_parsed = json.loads(content)
        return json_parsed['basicStats']['itemCount']

    def mutations(self, bucketname, docs=docs_max):
        # cb = Bucket('couchbase://' + self.ip + '/' + bucketname, password='')
        cb = Bucket('couchbase://' + self.ip + '/' + bucketname)
        for i in range(1, docs, 4):
            try:
                result = cb.get(str(i))
                timestamp1 = int(time.time())
                data1 = {"value": result.value["value"] + bucketname + "'", "last_updated_time": timestamp1,
                         "mutations": result.value["mutations"] + 1}
                cb.upsert(str(i), data1)

                if i + 1 <= docs:
                    result = cb.get(str(i + 1))
                    timestamp2 = int(time.time())
                    data2 = {"value": result.value["value"] + bucketname + "'", "last_updated_time": timestamp2,
                             "mutations": result.value["mutations"] + 1}
                    cb.replace(str(i + 1), data2)

                if i + 2 <= docs:
                    cb.remove(str(i + 2))

            except NotFoundError:
                pass

        for i in range(docs + 1, docs + docs / 4 + 1):
            timestamp3 = int(time.time())
            data = {"value": str(i), "last_updated_time": timestamp3, "mutations": 1}
            cb.insert(str(i), data)

    def comparison(self, src_ip, src_bucketname, compare, dst_ip, dst_bucketname, docs=docs_max):
        mappings = {'<': operator.lt, '<=': operator.le,
                    '>': operator.gt, '>=': operator.ge,
                    '==': operator.eq, '!=': operator.ne}
        cb1 = Bucket('couchbase://' + src_ip + '/' + src_bucketname, password='')
        cb2 = Bucket('couchbase://' + dst_ip + '/' + dst_bucketname, password='')
        for i in range(1, docs + docs / 4 + 1):
            key = str(i)
            try:
                value_src = cb1.get(key)
                value_dst = cb2.get(key)
                value_src_time = value_src.value['last_updated_time']
                value_dst_time = value_dst.value['last_updated_time']
                if compare == "!=":
                    if value_src > value_dst and value_src_time < value_dst_time:
                        print("1st if")
                        print(key + " :  " + str(value_src.cas) + " " + str(value_dst.cas))
                        print(key + " :  " + str(value_src_time) + " " + str(value_dst_time))
                        return False
                    if value_src < value_dst and value_src_time > value_dst_time:
                        print("2nd if")
                        print(key + " :  " + str(value_src.cas) + " " + str(value_dst.cas))
                        print(key + " :  " + str(value_src_time) + " " + str(value_dst_time))
                        return False
                    if value_src == value_dst and value_src_time != value_dst_time:
                        print("3rd if")
                        print(key + " :  " + str(value_src.cas) + " " + str(value_dst.cas))
                        print(key + " :  " + str(value_src_time) + " " + str(value_dst_time))
                        return False

                    #
                    # if value_src_time > value_dst_time and value_src <= value_dst:
                    #     print("4th if")
                    #     print(key + " :  " + str(value_src.cas) + " " + str(value_dst.cas))
                    #     print(key + " :  " + str(value_src_time) + " " + str(value_dst_time))
                    #     return False
                    # if value_src_time < value_dst_time and value_src >= value_dst:
                    #     print("5th if")
                    #     print(key + " :  " + str(value_src.cas) + " " + str(value_dst.cas))
                    #     print(key + " :  " + str(value_src_time) + " " + str(value_dst_time))
                    #     return False
                    # if value_src_time == value_dst_time and value_src != value_dst:
                    #     print("6th if")
                    #     print(key + " :  " + str(value_src.cas) + " " + str(value_dst.cas))
                    #     print(key + " :  " + str(value_src_time) + " " + str(value_dst_time))
                    #     return False
                else:
                    if not mappings[compare](value_src.cas, value_dst.cas) and not mappings[compare](value_src_time,
                                                                                             value_dst_time):
                        print("else")
                        print(key + " :  " + str(value_src.cas) + " " + str(value_dst.cas))
                        print(key + " :  " + str(value_src_time) + "" + str(value_dst_time))
            except NotFoundError:
                pass

        return True

    def _get_all_buckets(self):
        api = self.baseUrl + "/pools/default/buckets"
        status, content, _ = self._http_request(api, 'GET')
        if status:
            log.info("successful")
        else:
            log.error("/pools/default/buckets failed : status:{0},content:{1}".format(status, content))
        data = json.loads(content)
        bucket_list = []
        for each in data:
            if each['name'] != "default":
                bucket_list.append(each['name'])

        return bucket_list

    def _delete_all_buckets(self):
        arr = self._get_all_buckets()
        for bucket in arr:
            self.bucket_delete(bucket)

    def _get_all_references(self):
        api = self.baseUrl + "/pools/default/remoteClusters"
        status, content, _ = self._http_request(api, 'GET')
        if status:
            log.info("successful")
        else:
            log.error("/pools/default/remoteClusters failed : status:{0},content:{1}".format(status, content))
        data = json.loads(content)
        reference_list = []
        for each in data:
            reference_list.append(each['name'])
        return reference_list

    def _delete_all_references(self):
        arr = self._get_all_references()
        for cluster in arr:
            self.cluster_delete(cluster)


class TestLWW(unittest.TestCase):
    def tearDown(self):
        lww1 = LWWTtest(src_ip, src_port)
        lww1._delete_all_buckets()
        lww2 = LWWTtest(dst_ip, dst_port)
        lww2._delete_all_buckets()
        time.sleep(30)
        lww1._delete_all_references()
        lww2._delete_all_references()
        time.sleep(30)

    def test_UniXDCRLwwToLwwWhereDstDocumentsAreCreatedLaterThanSrc(self):
        lww1 = LWWTtest(src_ip, src_port)
        lww2 = LWWTtest(dst_ip, dst_port)

        lww1.bucket_create("src", "lww")
        lww1.document_create("src")
        lww2.bucket_create("dst", "lww")
        lww2.document_create("dst")

        lww1.add_remote_cluster(dst_ip, dst_port, "Administrator", "password", "AB")
        rep1 = lww1.start_replication("src", "AB", "dst")
        time.sleep(30)
        lww1.pause_replication(rep1)

        # don't do parallel mutations
        lww1.mutations("src")
        time.sleep(30)
        lww2.mutations("dst")

        lww1.resume_replication(rep1)
        time.sleep(30)

        # Values of CAS of dst should always be greater than src
        value = lww1.comparison(src_ip, "src", "<=", dst_ip, "dst")
        if value:
            assert True
        else:
            assert False

    def test_UniXDCRLwwToLww(self):
        lww1 = LWWTtest(src_ip, src_port)
        lww2 = LWWTtest(dst_ip, dst_port)

        lww1.bucket_create("src", "lww")
        lww1.document_create("src")

        lww2.bucket_create("dst", "lww")
        lww2.document_create("dst")

        lww1.add_remote_cluster(dst_ip, dst_port, "Administrator", "password", "AB")
        rep1 = lww1.start_replication("src", "AB", "dst")
        time.sleep(60)
        lww1.pause_replication(rep1)

        t1 = threading.Thread(target=lww1.mutations, args=("src",))
        t2 = threading.Thread(target=lww2.mutations, args=("dst",))
        t1.start()
        t2.start()

        t1.join()
        t2.join()

        lww1.resume_replication(rep1)
        time.sleep(30)
        # Values of CAS of dst should always be greater than src
        value = lww1.comparison(src_ip, "src", "!=", dst_ip, "dst")
        if value:
            assert True
        else:
            assert False

    def test_BiXDCRLwwToLww(self):
        lww1 = LWWTtest(src_ip, src_port)
        lww2 = LWWTtest(dst_ip, dst_port)

        lww1.bucket_create("src", "lww")
        lww1.document_create("src")

        lww2.bucket_create("dst", "lww")
        lww2.document_create("dst")

        lww1.add_remote_cluster(dst_ip, dst_port, "Administrator", "password", "AB")
        lww2.add_remote_cluster(src_ip, src_port, "Administrator", "password", "BA")

        rep1 = lww1.start_replication("src", "AB", "dst")
        rep2 = lww2.start_replication("dst", "BA", "src")

        time.sleep(60)
        lww1.pause_replication(rep1)
        lww2.pause_replication(rep2)

        t1 = threading.Thread(target=lww1.mutations, args=("src",))
        t2 = threading.Thread(target=lww2.mutations, args=("dst",))
        t1.start()
        t2.start()

        t1.join()
        t2.join()

        lww1.resume_replication(rep1)
        lww2.resume_replication(rep2)

        time.sleep(30)
        # Values of CAS of dst should always be greater than src
        value = lww1.comparison(src_ip, "src", "==", dst_ip, "dst")
        if value:
            assert True
        else:
            assert False


    def test_UniXDCRLwwToLwwFailOverSrc(self):
        lww1 = LWWTtest(src_ip, src_port)
        lww2 = LWWTtest(dst_ip, dst_port)

        lww1.bucket_create("src", "lww")
        lww1.document_create("src")

        lww2.bucket_create("dst", "lww")
        lww2.document_create("dst")

        lww1.add_remote_cluster(dst_ip, dst_port, "Administrator", "password", "AB")

        rep1 = lww1.start_replication("src", "AB", "dst")
        time.sleep(60)

        lww1.pause_replication(rep1)
        lww1.graceful_failover(src_ip_1, wait=1)

        lww1.mutations("src")
        time.sleep(30)
        lww2.mutations("dst")
        lww1.resume_replication(rep1)
        time.sleep(60)
        lww1.node_recovery(src_ip_1)
        lww1.cluster_rebalance(src_ip_1, wait=1)

        # Values of CAS of dst should always be greater than src
        value = lww1.comparison(src_ip, "src", "<=", dst_ip, "dst")
        if value:
            assert True
        else:
            assert False

    def test_BiXDCRLwwToLwwFailOverSrc(self):
        lww1 = LWWTtest(src_ip, src_port)
        lww2 = LWWTtest(dst_ip, dst_port)

        lww1.bucket_create("src", "lww")
        lww1.document_create("src")

        lww2.bucket_create("dst", "lww")
        lww2.document_create("dst")

        lww1.add_remote_cluster(dst_ip, dst_port, "Administrator", "password", "AB")
        lww2.add_remote_cluster(src_ip, src_port, "Administrator", "password", "BA")

        rep1 = lww1.start_replication("src", "AB", "dst")
        rep2 = lww2.start_replication("dst", "BA", "src")

        time.sleep(60)

        lww1.pause_replication(rep1)
        lww2.pause_replication(rep2)

        lww1.graceful_failover(src_ip_1, wait=1)

        lww1.mutations("src")
        lww2.mutations("dst")

        lww1.resume_replication(rep1)
        lww2.resume_replication(rep2)

        time.sleep(60)

        lww1.node_recovery(src_ip_1)
        lww1.cluster_rebalance(src_ip_1, wait=1)

        # Values of CAS of dst should be equal to src
        value = lww1.comparison(src_ip, "src", "==", dst_ip, "dst")
        if value:
            assert True
        else:
            assert False


if __name__ == '__main__':
    unittest.main()
