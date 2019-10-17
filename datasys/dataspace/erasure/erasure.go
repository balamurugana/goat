package erasure

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/balamurugana/goat/datasys/dataspace/disk"
	"github.com/balamurugana/goat/pkg/boundary"
	"github.com/balamurugana/goat/pkg/erasure"
)

type Part struct {
	erasure.Info
	ID string `json:"id"`
}

type DataInfo struct {
	Parts []Part `json:"parts"`
	Size  uint64 `json:"size"`
}

func (dataInfo DataInfo) getParts(offset, length int64) (requiredParts []Part, bytesToSkip, bytesToRead int64) {
	partSizes := make([]int64, len(dataInfo.Parts))
	for i, part := range dataInfo.Parts {
		partSizes[i] = int64(part.Size)
	}

	var startPart, endPart int64
	startPart, endPart, bytesToSkip, bytesToRead = boundary.CalcPartBoundaries(partSizes, offset, length)
	return dataInfo.Parts[startPart:endPart], bytesToSkip, bytesToRead
}

type Erasure struct {
	shardDisks []*disk.Disk
	minSuccess uint64
}

func NewErasure(shardDisks []*disk.Disk, minSuccess uint64) *Erasure {
	return &Erasure{
		shardDisks: shardDisks,
		minSuccess: minSuccess,
	}
}

func (ds *Erasure) SaveTempFile(filename string, data io.Reader, bitrotProtection bool, info *erasure.Info) (checksum string, err error) {
	count := info.DataCount + info.ParityCount
	if count != uint64(len(ds.shardDisks)) {
		return "", errors.New("info.DataCount+info.ParityCount != len(Erasure.shardDisks)")
	}

	shardIDMap := make(map[string]int)
	info.ShardIDs = make([]string, count)
	for i := range ds.shardDisks {
		shardID := ds.shardDisks[i].ID()
		info.ShardIDs[i] = shardID
		shardIDMap[shardID] = i
	}

	pipeReaders := make([]*io.PipeReader, count)
	pipeWriters := make([]*io.PipeWriter, count)
	writers := make([]io.Writer, count)
	for i := uint64(0); i < count; i++ {
		pipeReaders[i], pipeWriters[i] = io.Pipe()
		writers[i] = pipeWriters[i]
	}

	var mutex sync.Mutex
	getShardWriter := func(shardID string) (io.Writer, error) {
		mutex.Lock()
		i := shardIDMap[shardID]
		mutex.Unlock()
		return writers[i], nil
	}

	blockCount, _, _, lastShardSize := info.Compute()
	shardFileSize := lastShardSize + (blockCount-1)*info.ShardSize

	checksums := make([]string, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := range ds.shardDisks {
		wg.Add(1)
		go func(i int) {
			checksums[i], errs[i] = ds.shardDisks[i].SaveTempFile(filename, pipeReaders[i], shardFileSize, true)
			wg.Done()
		}(i)
	}

	shards := make([][]byte, count)
	for i := range shards {
		shards[i] = make([]byte, info.ShardSize)
	}

	shardSums, dataSum, err := erasure.Write(getShardWriter, shards, info, data, ds.minSuccess)

	for i := range pipeWriters {
		pipeWriters[i].Close()
	}
	wg.Wait()

	if err != nil {
		return "", err
	}

	successCount := uint64(len(shardSums))
	for i := range shardSums {
		if shardSums[i] == "" || shardSums[i] != checksums[i] {
			ds.shardDisks[i].RemoveTempFile(filename, true)
			successCount--
		}
	}

	if successCount < ds.minSuccess {
		return "", fmt.Errorf("multiple checksum mismatch error; %v != %v", shardSums, checksums)
	}

	return dataSum, err
}

func (ds *Erasure) RemoveTempFile(filename string, bitrotProtection bool) (err error) {
	errs := make([]error, len(ds.shardDisks))
	var wg sync.WaitGroup
	for i := range ds.shardDisks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = ds.shardDisks[i].RemoveTempFile(filename, bitrotProtection)
		}(i)
	}
	wg.Wait()

	successCount := uint64(0)
	for i := range errs {
		if errs[i] == nil {
			successCount++
		}
	}

	if successCount < ds.minSuccess {
		return fmt.Errorf("too many errors; %v", errs)
	}

	return nil
}

func (ds *Erasure) InitUpload(uploadID disk.UploadID) (err error) {
	errs := make([]error, len(ds.shardDisks))
	var wg sync.WaitGroup
	for i := range ds.shardDisks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = ds.shardDisks[i].InitUpload(uploadID)
		}(i)
	}
	wg.Wait()

	successCount := uint64(0)
	for i := range errs {
		if errs[i] == nil {
			successCount++
		}
	}

	if successCount >= ds.minSuccess {
		return nil
	}

	rerrs := make([]error, len(errs))
	for i := range errs {
		if errs[i] != nil {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rerrs[i] = ds.shardDisks[i].RevertInitUpload(uploadID)
			}(i)
		}
	}
	wg.Wait()

	return fmt.Errorf("too many errors; %v", errs)
}

func (ds *Erasure) UploadPart(uploadID disk.UploadID, partID, tempFile string) (err error) {
	errs := make([]error, len(ds.shardDisks))
	var wg sync.WaitGroup
	for i := range ds.shardDisks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = ds.shardDisks[i].UploadPart(uploadID, partID, tempFile)
		}(i)
	}
	wg.Wait()

	successCount := uint64(0)
	for i := range errs {
		if errs[i] == nil {
			successCount++
		}
	}

	if successCount >= ds.minSuccess {
		return nil
	}

	rerrs := make([]error, len(errs))
	for i := range errs {
		if errs[i] != nil {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rerrs[i] = ds.shardDisks[i].RevertUploadPart(uploadID, partID, tempFile)
			}(i)
		}
	}
	wg.Wait()

	return fmt.Errorf("too many errors; %v", errs)
}

// func (ds *Erasure) UploadPartCopy(uploadID UploadID, partID string, srcID DataID, offset int64, length uint64) (etag string, err error) {
// }
//

func (ds *Erasure) AbortUpload(uploadID disk.UploadID) (err error) {
	errs := make([]error, len(ds.shardDisks))
	var wg sync.WaitGroup
	for i := range ds.shardDisks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = ds.shardDisks[i].AbortUpload(uploadID)
		}(i)
	}
	wg.Wait()

	successCount := uint64(0)
	for i := range errs {
		if errs[i] == nil {
			successCount++
		}
	}

	if successCount >= ds.minSuccess {
		return nil
	}

	rerrs := make([]error, len(errs))
	for i := range errs {
		if errs[i] != nil {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rerrs[i] = ds.shardDisks[i].RevertAbortUpload(uploadID)
			}(i)
		}
	}
	wg.Wait()

	return fmt.Errorf("too many errors; %v", errs)
}

func (ds *Erasure) CompleteUpload(dataID disk.DataID, uploadID disk.UploadID, parts []Part) (*DataInfo, error) {
	size := uint64(0)
	diskParts := make([]disk.Part, len(parts))
	for i := range parts {
		diskParts[i].ID = parts[i].ID
		blockCount, _, _, lastShardSize := parts[i].Info.Compute()
		diskParts[i].Size = lastShardSize + (blockCount-1)*parts[i].ShardSize
		size += parts[i].Size
	}

	// NOTE: current assumption is all parts reside in same set of disks but may be in different order.

	errs := make([]error, len(ds.shardDisks))
	var wg sync.WaitGroup
	for i := range ds.shardDisks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = ds.shardDisks[i].CompleteUpload(dataID, uploadID, diskParts)
		}(i)
	}
	wg.Wait()

	successCount := uint64(0)
	for i := range errs {
		if errs[i] == nil {
			successCount++
		}
	}

	if successCount >= ds.minSuccess {
		return &DataInfo{
			Parts: parts,
			Size:  size,
		}, nil
	}

	rerrs := make([]error, len(errs))
	for i := range errs {
		if errs[i] != nil {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rerrs[i] = ds.shardDisks[i].RevertCompleteUpload(dataID, uploadID, diskParts)
			}(i)
		}
	}
	wg.Wait()

	return nil, fmt.Errorf("too many errors; %v", errs)
}

// func (ds *Erasure) ListParts(uploadID string) ([]Part, error) {
// }
//

func (ds *Erasure) Get(dataID disk.DataID, dataInfo *DataInfo, offset int64, length uint64) (rc io.ReadCloser, err error) {
	shardIDMap := make(map[string]int)
	for i := range ds.shardDisks {
		shardIDMap[ds.shardDisks[i].ID()] = i
	}

	var mutex sync.Mutex
	getShardReader := func(shardID string, offset, length int64) (io.ReadCloser, error) {
		mutex.Lock()
		i := shardIDMap[shardID]
		mutex.Unlock()
		return ds.shardDisks[i].Get(dataID, offset, uint64(length))
	}

	return newDataReader(getShardReader, dataInfo, offset, length)
}

// func (ds *Erasure) GetMetadata(ID string) (map[string][]string, error) {
// }
//
// func (ds *Erasure) Delete(ID string) error {
// }
//
// func (ds *Erasure) Copy(ID, srcID string, offset, length uint64, metadata map[string][]string) error {
// }
//
