package acl

type ACL string

const (
	Read        ACL = "READ"
	Write       ACL = "WRITE"
	ReadACP     ACL = "READ_ACP"
	WriteACP    ACL = "WRITE_ACP"
	FullControl ACL = "FULL_CONTROL"
)

type CannedACL string

const (
	Private                CannedACL = "private"
	PublicRead             CannedACL = "public-read"
	PublicReadWrite        CannedACL = "public-read-write"
	AWSExecRead            CannedACL = "aws-exec-read"
	AuthenticatedRead      CannedACL = "authenticated-read"
	BucketOwnerRead        CannedACL = "bucket-owner-read"
	BucketOwnerFullControl CannedACL = "bucket-owner-full-control"
	LogDeliveryWrite       CannedACL = "log-delivery-write"
)
