# xdcr-test
xdcr-test contains the tests that tests XDCR with LWW bucket with ongoing parallel mutations.

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
We currently verify using both CAS values and also a timestamp that is added by the script as part of the document inser/upsert/replace
