package s3

import (
	"time"

	"github.com/balamurugana/goat/datasys/namespace/s3/acl"
)

type Account struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Bucket struct {
	CreatedAt  time.Time `json:"createdAt"`
	ObjectLock bool      `json:"objectLock"`
	Owner      Account   `json:"owner"`
	Region     string    `json:"region"`
}

type ACL struct {
	ACLs      []acl.ACL     `json:"acl"`
	CannedACL acl.CannedACL `json:"cannedACL"`
}

type SSEType string

const (
	AWSKMS SSEType = "aws:kms"
	AES256 SSEType = "AES256"
	SSEC   SSEType = "SSE-C"
)

type SSE struct {
	EncryptionContext string  `json:"encryptionContext"`
	CustomerAlgorithm string  `json:"customerAlgorithm"`
	CustomerKey       string  `json:"customerKey"`
	CustomerKeyMD5    string  `json:"customerKeyMD5"`
	KMSKeyID          string  `json:"kmsKeyID"`
	Type              SSEType `json:"type"`
}

type LockMode string

const (
	Compliance LockMode = "COMPLIANCE"
	Governance LockMode = "GOVERNANCE"
)

type ObjectLock struct {
	LegalHold       bool      `json:"legalHold"`
	LockMode        LockMode  `json:"lockMode"`
	RetainUntilDate time.Time `json:"retainUntilDate"`
}

type Object struct {
	CacheControl            string    `json:"cacheControl"`
	ContentDisposition      string    `json:"contentDisposition"`
	ContentEncoding         string    `json:"contentEncoding"`
	ContentLanguage         string    `json:"contentLanguage"`
	ContentType             string    `json:"contentType"`
	ETag                    string    `json:"etag"`
	Expires                 string    `json:"expires"`
	ModifiedAt              time.Time `json:"modifiedAt"`
	Owner                   Account   `json:"owner"`
	Size                    uint64    `json:"size"`
	StorageClass            string    `json:"storageClass"`
	WebsiteRedirectLocation string    `json:"websiteRedirectLocation"`
}

type Upload struct {
	ACL        ACL        `json:"acl"`
	CreatedAt  time.Time  `json:"createdAt"`
	Initator   Account    `json:"initiator"`
	Object     Object     `json:"object"`
	ObjectLock ObjectLock `json:"objectLock"`
	SSE        SSE        `json:"sse"`
}

type Part struct {
	ETag         string    `json:"etag"`
	Initator     Account   `json:"initiator"`
	ModifiedAt   time.Time `json:"modifiedAt"`
	Owner        Account   `json:"owner"`
	Size         uint64    `json:"size"`
	SSE          SSE       `json:"sse"`
	StorageClass string    `json:"storageClass"`
}
