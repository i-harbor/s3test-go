package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var s3AccesccKey = "xxx"
var s3SecretKey = "xxx"

var letters = []byte("abcdefghjkmnpqrstuvwxyz123456789")
var longLetters = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ=_")

func init() {
	rand.Seed(time.Now().Unix())
}

func main() {
	bucket := "test-harbor-s3"
	key := "b/test.data"
	filename := "./test.data"
	prefix := "a/"
	mbNum := 16
	multipartKey := fmt.Sprintf("%ss3_object_%dMB.data", prefix, mbNum)
	fmt.Println("s3test starting...")

	if err := MakeFile(filename, 16); err != nil {
		fmt.Fprintf(os.Stderr, "MakeFile err: %v\n", err)
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Endpoint:    aws.String("http://s3.obs.cstcloud.cn"),
		Region:      aws.String("us-east-1"),
		DisableSSL:  aws.Bool(true),
		Credentials: credentials.NewStaticCredentials(s3AccesccKey, s3SecretKey, ""),
	}))
	svc := s3.New(sess)

	TestCreateBucket(svc, bucket)
	TestListBuckets(svc)
	TestPutObject(svc, bucket, key, filename)
	TestListObjectsV2(svc, bucket, "a/")
	TestHeadObject(svc, bucket, key)
	TestGetObject(svc, bucket, key)
	TestDeleteObject(svc, bucket, key)
	TestMultipartUpload(svc, bucket, multipartKey, filename, 1024*1024*5)
	TestDeleteObjects(svc, bucket, []string{key, multipartKey})
	TestDeleteBucket(svc, bucket)

	if err := RemoveFile(filename); err != nil {
		fmt.Fprintf(os.Stderr, "RemoveFile err: %v\n", err)
	}
	fmt.Println("s3test ending...")
}

// RandLow 随机字符串，包含 1~9 和 a~z - [i,l,o]
func RandLow(n int) []byte {
	if n <= 0 {
		return []byte{}
	}
	b := make([]byte, n)
	arc := uint8(0)
	if _, err := rand.Read(b[:]); err != nil {
		return []byte{}
	}
	for i, x := range b {
		arc = x & 31
		b[i] = letters[arc]
	}
	return b
}

// MakeFile generate a file
func MakeFile(filename string, mbNum int) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	for i := 0; i < mbNum; i++ {
		s := RandLow(1024 * 1024)
		f.Write(s)
	}
	return nil
}

// RemoveFile remove a file
func RemoveFile(filename string) error {
	err := os.Remove(filename)
	if err != nil {
		return err
	}
	return nil
}

// ReadfileGenerater generater mode to read a file
func ReadfileGenerater(filename string, partSize int64) (c chan []byte) {
	c = make(chan []byte, 1)
	go func() {
		f, err := os.Open(filename)
		if err != nil {
			return
		}
		defer f.Close()

		for {
			buf := make([]byte, partSize) // make a new "buf" each loop
			n, err := io.ReadFull(f, buf)
			if err != nil {
				if err == io.EOF {
					close(c)
					break
				} else if err == io.ErrUnexpectedEOF {
					c <- buf[:n]
					close(c)
					break
				}
			}
			c <- buf[:n]
		}
	}()
	return c
}

func calculateMd5(data []byte) string {
	b := calculateMd5Bytes(data)
	sMd5 := hex.EncodeToString(b)
	return sMd5
}

func calculateMd5Bytes(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

func calculateFileMd5(filename string) (*string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	hash := md5.New()
	for buf, reader := make([]byte, 1024*1024*5), bufio.NewReader(f); ; {
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		hash.Write(buf[:n])
	}
	b := hash.Sum(nil)
	sMd5 := hex.EncodeToString(b)
	return &sMd5, nil
}

func calculateMultipartS3ETag(filename string, chunkSize int64) (*string, error) {
	hash := md5.New()
	counter := 0

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	for buf := make([]byte, chunkSize); ; {
		n, err := io.ReadFull(f, buf)
		if err != nil {
			if err == io.EOF {
				break
			} else if err != io.ErrUnexpectedEOF { // 读了部分数据，已读到文件结尾
				return nil, err
			}
		}
		b := calculateMd5Bytes(buf[:n])
		hash.Write(b)
		counter++
	}

	hb := hash.Sum(nil)
	sMd5 := hex.EncodeToString(hb)
	etag := fmt.Sprintf(`"%s-%d"`, sMd5, counter)
	return &etag, nil
}

// TestCreateBucket test CreateBucket api
func TestCreateBucket(svc *s3.S3, bucket string) {
	fmt.Println("@@@ CreateBucket")
	r, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
		ACL:    aws.String(s3.BucketCannedACLPrivate),
	})

	if err != nil {
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou {
			fmt.Printf("[Ok] TestCreateBucket; bucket(%s) AlreadyOwnedByYou \n", bucket)
			return
		} else if ok && aerr.Code() == request.CanceledErrorCode {
			fmt.Fprintf(os.Stderr, "[Error] TestCreateBucket; request canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestCreateBucket; failed to request, %v\n", err)
		}
		os.Exit(1)
	}
	fmt.Printf("[Ok] TestCreateBucket; Location=%s \n", *r.Location)
}

// TestDeleteBucket test DeleteBucket api
func TestDeleteBucket(svc *s3.S3, bucket string) {
	fmt.Println("@@@ TestDeleteBucket")
	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			fmt.Fprintf(os.Stderr, "[Error] TestDeleteBucket; request canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestDeleteBucket; failed to request, %v\n", err)
		}
	}
	fmt.Printf("[Ok] TestDeleteBucket")
}

// TestListBuckets test ListBuckets api
func TestListBuckets(svc *s3.S3) {
	fmt.Println("@@@ TestListBuckets")
	r, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "[Error] TestListBuckets; upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestListBuckets; failed to upload object, %v\n", err)
		}
		os.Exit(1)
	}
	buckets := r.Buckets
	for i, b := range buckets {
		fmt.Printf("[Ok] TestListBuckets; %d: Name=%s; CreationDate=%s; Owner=%s \n", i, *b.Name, b.CreationDate.String(), *r.Owner.DisplayName)
	}
}

// TestListObjectsV2 test ListObjects V2 api
func TestListObjectsV2(svc *s3.S3, bucket string, prefix string) {
	fmt.Println("@@@ TestListObjectsV2")
	r, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Prefix:     aws.String(prefix),
		Delimiter:  aws.String("/"),
		FetchOwner: aws.Bool(true),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "[Error] TestListObjectsV2; upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestListObjectsV2; Failed to list objects v2, %v\n", err)
		}
		os.Exit(1)
	}
	contents := r.Contents
	for i, obj := range contents {
		fmt.Printf("%d: Key=%s; LastModified=%s; Owner=%s \n", i, *obj.Key, obj.LastModified.String(), *obj.Owner.DisplayName)
		break
	}
	fmt.Printf("[OK] TestListObjectsV2.\n")
}

// TestPutObject test PutObject api
func TestPutObject(svc *s3.S3, bucket string, key string, filename string) {
	fmt.Println("@@@ TestPutObject")
	f, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Error] TestPutObject; failed to open file: %s, err=%v\n", filename, err)
	}
	defer f.Close()

	r, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "[Error] TestPutObject; upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestPutObject; failed to put object, %v\n", err)
		}
		os.Exit(1)
	}
	fileMd5, err := calculateFileMd5(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to calculateFileMd5, %v\n", err)
	}
	if *fileMd5 == *r.ETag {
		fmt.Printf("[Ok] TestPutObject; ETag(%s) == fileMd5(%s)\n", *r.ETag, *fileMd5)
	} else {
		fmt.Printf("[Failed] TestPutObject; ETag(%s) != fileMd5(%s)\n", *r.ETag, *fileMd5)
	}
}

// TestGetObject test GetObject api
func TestGetObject(svc *s3.S3, bucket string, key string) {
	fmt.Println("@@@ TestGetObject")
	r, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "[Error] TestGetObject; upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestGetObject; failed to get object, %v\n", err)
		}
		os.Exit(1)
	}
	fmt.Printf("[Ok] TestGetObject; ETag=%s; LastModified=%s \n", *r.ETag, r.LastModified.String())
}

// TestDeleteObject test DeleteObject api
func TestDeleteObject(svc *s3.S3, bucket string, key string) {
	fmt.Println("@@@ TestDeleteObject")
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "[Error] TestDeleteObject; delete canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestDeleteObject; failed to delete object, %v\n", err)
		}
		os.Exit(1)
	}
	fmt.Printf("[Ok] TestDeleteObject; \n")
}

// TestDeleteObjects test DeleteObjects api
func TestDeleteObjects(svc *s3.S3, bucket string, keys []string) {
	fmt.Println("@@@ TestDeleteObjects")
	objects := make([]*s3.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objects[i] = &s3.ObjectIdentifier{
			Key: aws.String(key),
		}
	}
	r, err := svc.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(false),
		},
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			fmt.Fprintf(os.Stderr, "[Error] TestDeleteObjects; delete canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestDeleteObjects; failed to delete object, %v\n", err)
		}
		os.Exit(1)
	}
	fmt.Printf("[Ok] TestDeleteObjects \n Delete objects: %v; \n Error objects: %v \n", r.Deleted, r.Errors)
}

// TestHeadObject test HeadObject api
func TestHeadObject(svc *s3.S3, bucket string, key string) {
	fmt.Println("@@@ TestHeadObject")
	r, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "[Error] TestHeadObject; request canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestHeadObject; failed to request, %v\n", err)
		}
		os.Exit(1)
	}
	fmt.Printf("[Ok] TestHeadObject; ETag=%s; LastModified=%s \n", *r.ETag, r.LastModified.String())
}

// CreateMultipartUpload test CreateMultipartUpload api
func CreateMultipartUpload(svc *s3.S3, bucket string, key string) (*string, error) {
	r, err := svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return nil, err
	}
	return r.UploadId, nil
}

// UploadPart test UploadPart api
func UploadPart(svc *s3.S3, bucket string, key string, uploadID string, partNumber int64, data []byte) (*s3.CompletedPart, error) {
	r, err := svc.UploadPart(&s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int64(partNumber),
		Body:       bytes.NewReader(data),
	})

	if err != nil {
		return nil, err
	}

	return &s3.CompletedPart{
		PartNumber: aws.Int64(partNumber),
		ETag:       aws.String(*r.ETag),
	}, nil
}

// CompleteMultipartUpload test CompleteMultipartUpload api
func CompleteMultipartUpload(svc *s3.S3, bucket string, key string, uploadID string, parts []*s3.CompletedPart) (*string, error) {
	mu := s3.CompletedMultipartUpload{
		Parts: parts,
	}
	r, err := svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &mu,
	})

	if err != nil {
		return nil, err
	}
	return r.ETag, nil
}

// AbortMultipartUpload test AbortMultipartUpload api
func AbortMultipartUpload(svc *s3.S3, bucket string, key string, uploadID string) error {
	_, err := svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})

	if err != nil {
		return err
	}
	return nil
}

// UploadPartFile upload all parts of file
func UploadPartFile(svc *s3.S3, bucket string, key string, uploadID string, filename string, partSize int64) ([]*s3.CompletedPart, error) {
	var partNumber int64 = 1
	parts := make([]*s3.CompletedPart, 0)
	for data := range ReadfileGenerater(filename, partSize) {
		part, err := UploadPart(svc, bucket, key, uploadID, partNumber, data)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "[OK] UploadPart; part= %v\n", part)
		parts = append(parts, part)
		partNumber++
	}

	// f, err := os.Open(filename)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "[Error] open file, %v\n", err)
	// }
	// defer f.Close()

	// for buf := make([]byte, partSize); ; {
	// 	n, err := io.ReadFull(f, buf)
	// 	if err != nil {
	// 		if err == io.EOF {
	// 			break
	// 		} else if err != io.ErrUnexpectedEOF {
	// 			return nil, err
	// 		}
	// 	}

	// 	part, err := UploadPart(svc, bucket, key, uploadID, partNumber, buf[:n])
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	fmt.Fprintf(os.Stderr, "[OK] UploadPart; part= %v\n", part)
	// 	parts = append(parts, part)
	// 	partNumber++
	// }
	return parts, nil
}

// TestMultipartUpload A complete multipart upload test
func TestMultipartUpload(svc *s3.S3, bucket string, key string, filename string, partSize int64) {

	fmt.Println("@@@ TestMultipartUpload")
	fmt.Println("@@@ CreateMultipartUpload")
	uploadID, err := CreateMultipartUpload(svc, bucket, key)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			fmt.Fprintf(os.Stderr, "[Error] CreateMultipartUpload; request canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] CreateMultipartUpload; failed to request, %v\n", err)
		}
		os.Exit(1)
	}
	fmt.Printf("[Ok] CreateMultipartUpload; UploadId=%s\n", *uploadID)

	fmt.Println("@@@ UploadParts")
	parts, err := UploadPartFile(svc, bucket, key, *uploadID, filename, partSize)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			fmt.Fprintf(os.Stderr, "[Error] TestUploadPart; request canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestUploadPart; failed to request, %v\n", err)
		}
		if err = AbortMultipartUpload(svc, bucket, key, *uploadID); err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				fmt.Fprintf(os.Stderr, "[Error] AbortMultipartUpload; request canceled due to timeout, %v\n", err)
			} else {
				fmt.Fprintf(os.Stderr, "[Failed] AbortMultipartUpload; failed to request, %v\n", err)
			}
		}
		os.Exit(1)
	}
	fmt.Printf("[Ok] TestUploadPart; Parts=%v\n", parts)

	fmt.Println("@@@ TestCompleteMultipartUpload")
	etag, err := CompleteMultipartUpload(svc, bucket, key, *uploadID, parts)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			fmt.Fprintf(os.Stderr, "[Error] TestCompleteMultipartUpload; request canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "[Failed] TestCompleteMultipartUpload; failed to request, %v\n", err)
		}
		os.Exit(1)
	}
	fmt.Printf("[Ok] CompleteMultipartUpload; ETag=%s \n", *etag)

	fileEtag, err := calculateMultipartS3ETag(filename, partSize)
	if err != nil {
		fmt.Printf("calculateMultipartS3ETag error: %v \n", err)
	}
	if *fileEtag == *etag {
		fmt.Printf("[Ok] CompleteMultipartUpload; ETag(%s) == fileEtag(%s) \n", *etag, *fileEtag)
	} else {
		fmt.Printf("[Warning] CompleteMultipartUpload; ETag(%s) != fileEtag(%s) \n", *etag, *fileEtag)
	}

	fmt.Println("[Ok] TestMultipartUpload")
}
