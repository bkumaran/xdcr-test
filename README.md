# xdcr-test
xdcr-test contains the script that tests XDCR with LWW bucket with ongoing parallel mutations and also verfies the CAS values.

## Requirements ##
Python : 2.7 or greater

Install Couchbase python SDK , See [here](http://developer.couchbase.com/documentation/server/current/sdk/python/start-using-sdk.html)

## Usage ##
```
git clone https://github.com/bkumaran/xdcr-test.git
cd xdcr-test
python parallel_mutations.py
```

## Verification ##
We currently verify using both CAS values and also a timestamp that is added by the script as part of the document insert/upsert/replace

Here we compare whether all the docs in the 'dst' bucket has a CAS value more than that of 'src' bucket
```value = lww1.comparison(src_ip,"src","<=",dst_ip,"dst")
    if value:
        print ("passed")
    else:
        print ("failed")
```
