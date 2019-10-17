# Data space meta data
## Object level erasure meta data
VERSIONID-data.json
```json
{
    "parts": [
        {
            "dataCount": 2,
            "id": "3",
            "parityCount": 2,
            "shardIDs": [
                "ds1",
                "ds2",
                "ds3",
                "ds4"
            ],
            "shardSize": 1048576,
            "size": 133362
        },
        {
            "dataCount": 2,
            "id": "8",
            "parityCount": 2,
            "shardIDs": [
                "ds2",
                "ds4",
                "ds3",
                "ds1"
            ],
            "shardSize": 1048576,
            "size": 1566841
        }
    ],
    "size": 1700203
}
```

## Shard level meta data
```json
{
    "parts": [
        {
            "id": "3",
            "size": 66681
        },
        {
            "id": "8",
            "size": 783421
        }
    ],
    "size": 850102
}
```

# Name space meta data
## Bucket
bucket.json
```json
{
    "createdAt": "TIME",
    "objectLock": true,
    "owner": {
        "id": "ID",
        "name": "NAME"
    },
    "region": "eu-west-1a"
}
```
acl.json
```json
{
    "acl": [
        "READ",
        "WRITE",
        "READ_ACP",
        "WRITE_ACP",
        "FULL_CONTROL"
    ],
    "cannedACL": "CannedACL"
}
```
## Object
VERSIONID.json
```json
{
    "cacheControl": "CacheControl",
    "contentDisposition": "ContentDisposition",
    "contentEncoding": "ContentEncoding",
    "contentLanguage": "ContentLanguage",
    "contentType": "ContentType",
    "etag": "ETAG",
    "expires": "Expires",
    "modifiedAt": "TIME",
    "owner": {
        "id": "ID",
        "name": "NAME"
    },
    "size": 1566841,
    "storageClass": "StorageClass",
    "websiteRedirectLocation": "WebsiteRedirectLocation"
}
```
VERSIONID-acl.json
```json
{
    "acl": [
        "READ",
        "WRITE",
        "READ_ACP",
        "WRITE_ACP",
        "FULL_CONTROL"
    ],
    "cannedACL": "CannedACL"
}
```
VERSIONID-see.json
```json
{
    "context": "SSEKMSEncryptionContext",
    "customerAlgorithm": "SSECustomerAlgorithm",
    "customerKey": "SSECustomerKey",
    "customerKeyMD5": "SSECustomerKeyMD5",
    "kmsKeyId": "SSEKMSKeyId",
    "type": "ServerSideEncryption"
}
```
VERSIONID-object-lock.json
```json
{
    "objectLockLegalHoldStatus": "ObjectLockLegalHoldStatus",
    "objectLockMode": "ObjectLockMode",
    "objectLockRetainUntilDate": "ObjectLockRetainUntilDate"
}
```
VERSIONID-tagging.json
```json
{
    "tagging": "Tagging"
}
```

## Multipart Upload
UPLOADID.json
```json
{
    "acl": [
        "READ",
        "WRITE",
        "READ_ACP",
        "WRITE_ACP",
        "FULL_CONTROL"
    ],
    "cacheControl": "CacheControl",
    "cannedACL": "CannedACL",
    "contentDisposition": "ContentDisposition",
    "contentEncoding": "ContentEncoding",
    "contentLanguage": "ContentLanguage",
    "contentType": "ContentType",
    "createdAt": "TIME",
    "expires": "Expires",
    "initiator": {
        "id": "ID",
        "name": "NAME"
    },
    "objectLock": {
        "objectLockLegalHoldStatus": "ObjectLockLegalHoldStatus",
        "objectLockMode": "ObjectLockMode",
        "objectLockRetainUntilDate": "ObjectLockRetainUntilDate"
    },
    "owner": {
        "id": "ID",
        "name": "NAME"
    },
    "sse": {
        "context": "SSEKMSEncryptionContext",
        "customerAlgorithm": "SSECustomerAlgorithm",
        "customerKey": "SSECustomerKey",
        "customerKeyMD5": "SSECustomerKeyMD5",
        "kmsKeyId": "SSEKMSKeyId",
        "type": "ServerSideEncryption"
    },
    "storageClass": "StorageClass",
    "tagging": {
        "tagging": "Tagging"
    },
    "websiteRedirectLocation": "WebsiteRedirectLocation"
}
```

### Part meta data
N.part
```json
{
    "etag": "ETag",
    "initiator": {
        "id": "ID",
        "name": "NAME"
    },
    "modifiedAt": "TIME",
    "owner": {
        "id": "ID",
        "name": "NAME"
    },
    "size": 133362,
    "sse": {
        "context": "SSEKMSEncryptionContext",
        "customerAlgorithm": "SSECustomerAlgorithm",
        "customerKey": "SSECustomerKey",
        "customerKeyMD5": "SSECustomerKeyMD5",
        "kmsKeyId": "SSEKMSKeyId",
        "type": "ServerSideEncryption"
    },
    "storageClass": "StorageClass"
}
```
