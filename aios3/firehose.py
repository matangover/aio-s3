from .bucket import _SIGNATURES, Request, SIGNATURE_V4, Request
from .errors import AWSException
import asyncio
import json
import aiohttp
import base64

class Firehose(object):

    def __init__(self, *,
                 aws_key, aws_secret,
                 aws_region,
                 signature=SIGNATURE_V4):
        self._aws_sign_data = {
            'aws_key': aws_key,
            'aws_secret': aws_secret,
            'aws_region': aws_region,
            'aws_service': 'firehose'
#            'aws_bucket': name,
            }
        self._host = "firehose.{}.amazonaws.com".format(aws_region)
        self._signature = signature

    def _request(self, req):
        _SIGNATURES[self._signature](req, **self._aws_sign_data)
        req.headers['CONTENT-LENGTH'] = str(len(req.payload))
        print("Sending request:", req.verb, req.url, req.headers, req.payload)
        return aiohttp.request(
            req.verb,
            req.url,
            headers=req.headers,
            data=req.payload)

    async def put_record(self, DeliveryStreamName, Record):
        Record["Data"] = base64.b64encode(Record["Data"].encode("utf-8")).decode("ascii")
        data = json.dumps({"DeliveryStreamName": DeliveryStreamName, "Record": Record}).encode("utf-8")
        async with self._request(
            HTTPSRequest("POST", "/", {}, {"HOST": self._host, 'x-amz-target': 'Firehose_20150804.PutRecord', 'Content-Type': 'application/x-amz-json-1.1'}, data)
        ) as result:
            data = await result.text()
            if result.status != 200:
                raise AWSException("HTTP Error {}: {}".format(result.status, data))

            return data

class HTTPSRequest(Request):
    @property
    def url(self):
        query_string = "?" + self.query_string if self.query_string else ""
        return 'https://{0.headers[HOST]}{0.resource}{1}' \
            .format(self, query_string)
