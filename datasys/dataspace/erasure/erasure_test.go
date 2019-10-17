package erasure

import (
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/balamurugana/goat/datasys/disk"
	"github.com/balamurugana/goat/pkg/erasure"
	xhash "github.com/balamurugana/goat/pkg/hash"
	xrand "github.com/balamurugana/goat/pkg/rand"
)

const MiB = 1 * 1024 * 1024

func TestSaveTempFile(t *testing.T) {
	testCases := []struct {
		info     *erasure.Info
		checksum string
	}{
		{
			info: &erasure.Info{
				DataCount:   1,
				ParityCount: 3,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "53e488c20a4168a2d093f7d221e649582f87ccb54124bf85afa4fb5619211621",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 2,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 7,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        MiB,
				ShardSize:   MiB,
			},
			checksum: "85e459b0a2124d84a5caf33c7db50bd357ed6779fb211684c5bded824f99e7b5",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        4 * MiB,
				ShardSize:   MiB,
			},
			checksum: "782a82eeeec6117e8cf251539b6152859732943867949f0207ff7eec8a7e7278",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        2 * 4 * MiB,
				ShardSize:   MiB,
			},
			checksum: "e9e4dc0e71b4c49ea20398c527de26854e00e9fda90a666b1cad2baef08d4c21",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283 + MiB,
				ShardSize:   MiB,
			},
			checksum: "4ae7f09322921debd873f53bd726b0c0e6d04d96e8496166cb015d3ed12965df",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283 + 4*MiB,
				ShardSize:   MiB,
			},
			checksum: "bd77d467172842b58066fffbf126bd6fa8c8e04ff3607c4500ec6f35c53f1fc1",
		},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				id := xrand.NewID(8).String()
				workDir := id
				if err := os.Mkdir(workDir, os.ModePerm); err != nil {
					t.Fatal(err)
				}

				defer func() {
					os.RemoveAll(workDir)
				}()

				count := testCase.info.DataCount + testCase.info.ParityCount
				shardDisks := make([]*disk.Disk, count)
				var err error
				for j := uint64(0); j < count; j++ {
					id := fmt.Sprintf("d%v", j)
					dataDir := path.Join(workDir, id)
					if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
						t.Fatalf("%v: %v", id, err)
					}

					shardDisks[j], err = disk.NewDisk(id, dataDir)
					if err != nil {
						t.Fatalf("%v: %v", id, err)
					}
				}

				erasureDisk := NewErasure(shardDisks, count)

				tempFilename := disk.NewTempFilename()
				checksum, err := erasureDisk.SaveTempFile(tempFilename, randReader(), true, testCase.info)
				if err != nil {
					t.Fatal(err)
				}

				if checksum != testCase.checksum {
					t.Fatalf("mismatch: checksum: expected: %v, got: %v", testCase.checksum, checksum)
				}
			},
		)

	}
}

func TestRemoveTempFile(t *testing.T) {
	testCases := []struct {
		info     *erasure.Info
		checksum string
	}{
		{
			info: &erasure.Info{
				DataCount:   1,
				ParityCount: 3,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "53e488c20a4168a2d093f7d221e649582f87ccb54124bf85afa4fb5619211621",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 2,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 7,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        MiB,
				ShardSize:   MiB,
			},
			checksum: "85e459b0a2124d84a5caf33c7db50bd357ed6779fb211684c5bded824f99e7b5",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        4 * MiB,
				ShardSize:   MiB,
			},
			checksum: "782a82eeeec6117e8cf251539b6152859732943867949f0207ff7eec8a7e7278",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        2 * 4 * MiB,
				ShardSize:   MiB,
			},
			checksum: "e9e4dc0e71b4c49ea20398c527de26854e00e9fda90a666b1cad2baef08d4c21",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283 + MiB,
				ShardSize:   MiB,
			},
			checksum: "4ae7f09322921debd873f53bd726b0c0e6d04d96e8496166cb015d3ed12965df",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283 + 4*MiB,
				ShardSize:   MiB,
			},
			checksum: "bd77d467172842b58066fffbf126bd6fa8c8e04ff3607c4500ec6f35c53f1fc1",
		},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				id := xrand.NewID(8).String()
				workDir := id
				if err := os.Mkdir(workDir, os.ModePerm); err != nil {
					t.Fatal(err)
				}

				defer func() {
					os.RemoveAll(workDir)
				}()

				count := testCase.info.DataCount + testCase.info.ParityCount
				shardDisks := make([]*disk.Disk, count)
				var err error
				for j := uint64(0); j < count; j++ {
					id := fmt.Sprintf("d%v", j)
					dataDir := path.Join(workDir, id)
					if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
						t.Fatalf("%v: %v", id, err)
					}

					shardDisks[j], err = disk.NewDisk(id, dataDir)
					if err != nil {
						t.Fatalf("%v: %v", id, err)
					}
				}

				erasureDisk := NewErasure(shardDisks, count)

				tempFilename := disk.NewTempFilename()
				checksum, err := erasureDisk.SaveTempFile(tempFilename, randReader(), true, testCase.info)
				if err != nil {
					t.Fatal(err)
				}

				if checksum != testCase.checksum {
					t.Fatalf("mismatch: checksum: expected: %v, got: %v", testCase.checksum, checksum)
				}

				if err = erasureDisk.RemoveTempFile(tempFilename, true); err != nil {
					t.Fatal(err)
				}

				if err = erasureDisk.RemoveTempFile(tempFilename, true); err == nil {
					t.Fatalf("mismatch: expected: <error>, got: <nil>")
				}
			},
		)
	}
}

func TestInitUpload(t *testing.T) {
	t.Run(
		"test0",
		func(t *testing.T) {
			id := xrand.NewID(8).String()
			workDir := id
			if err := os.Mkdir(workDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(workDir)
			}()

			count := uint64(7)
			shardDisks := make([]*disk.Disk, count)
			var err error
			for j := uint64(0); j < count; j++ {
				id := fmt.Sprintf("d%v", j)
				dataDir := path.Join(workDir, id)
				if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
					t.Fatalf("%v: %v", id, err)
				}

				shardDisks[j], err = disk.NewDisk(id, dataDir)
				if err != nil {
					t.Fatalf("%v: %v", id, err)
				}
			}

			erasureDisk := NewErasure(shardDisks, count)

			uploadID := disk.NewUploadID()

			if err = erasureDisk.InitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err = erasureDisk.AbortUpload(uploadID); err != nil {
				t.Fatal(err)
			}
		},
	)
}

func TestUploadPart(t *testing.T) {
	testCases := []struct {
		info     *erasure.Info
		checksum string
	}{
		{
			info: &erasure.Info{
				DataCount:   1,
				ParityCount: 3,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "53e488c20a4168a2d093f7d221e649582f87ccb54124bf85afa4fb5619211621",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 2,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 7,
				Size:        32283,
				ShardSize:   MiB,
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        MiB,
				ShardSize:   MiB,
			},
			checksum: "85e459b0a2124d84a5caf33c7db50bd357ed6779fb211684c5bded824f99e7b5",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        4 * MiB,
				ShardSize:   MiB,
			},
			checksum: "782a82eeeec6117e8cf251539b6152859732943867949f0207ff7eec8a7e7278",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        2 * 4 * MiB,
				ShardSize:   MiB,
			},
			checksum: "e9e4dc0e71b4c49ea20398c527de26854e00e9fda90a666b1cad2baef08d4c21",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283 + MiB,
				ShardSize:   MiB,
			},
			checksum: "4ae7f09322921debd873f53bd726b0c0e6d04d96e8496166cb015d3ed12965df",
		},
		{
			info: &erasure.Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283 + 4*MiB,
				ShardSize:   MiB,
			},
			checksum: "bd77d467172842b58066fffbf126bd6fa8c8e04ff3607c4500ec6f35c53f1fc1",
		},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				id := xrand.NewID(8).String()
				workDir := id
				if err := os.Mkdir(workDir, os.ModePerm); err != nil {
					t.Fatal(err)
				}

				defer func() {
					os.RemoveAll(workDir)
				}()

				count := testCase.info.DataCount + testCase.info.ParityCount
				shardDisks := make([]*disk.Disk, count)
				var err error
				for j := uint64(0); j < count; j++ {
					id := fmt.Sprintf("d%v", j)
					dataDir := path.Join(workDir, id)
					if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
						t.Fatalf("%v: %v", id, err)
					}

					shardDisks[j], err = disk.NewDisk(id, dataDir)
					if err != nil {
						t.Fatalf("%v: %v", id, err)
					}
				}

				erasureDisk := NewErasure(shardDisks, count)

				tempFilename := disk.NewTempFilename()
				checksum, err := erasureDisk.SaveTempFile(tempFilename, randReader(), true, testCase.info)
				if err != nil {
					t.Fatal(err)
				}

				if checksum != testCase.checksum {
					t.Fatalf("mismatch: checksum: expected: %v, got: %v", testCase.checksum, checksum)
				}

				uploadID := disk.NewUploadID()

				if err = erasureDisk.UploadPart(uploadID, "211", tempFilename); err == nil {
					t.Fatalf("mismatch: expected: <error>, got: <nil>")
				}

				if err = erasureDisk.InitUpload(uploadID); err != nil {
					t.Fatal(err)
				}

				if err = erasureDisk.UploadPart(uploadID, "211", tempFilename); err != nil {
					t.Fatal(err)
				}

				if err = erasureDisk.UploadPart(uploadID, "211", tempFilename); err == nil {
					t.Fatalf("mismatch: expected: <error>, got: <nil>")
				}
			},
		)
	}
}

func TestAbortUpload(t *testing.T) {
	testCases := []struct {
		parts []Part
	}{
		{[]Part{newPart("1", 16279)}},
		{[]Part{newPart("3", 16279), newPart("8", 10992), newPart("1", 25489)}},
	}

	id := xrand.NewID(8).String()
	workDir := id
	if err := os.Mkdir(workDir, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	defer func() {
		os.RemoveAll(workDir)
	}()

	count := uint64(7)
	shardDisks := make([]*disk.Disk, count)
	var err error
	for j := uint64(0); j < count; j++ {
		id := fmt.Sprintf("d%v", j)
		dataDir := path.Join(workDir, id)
		if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
			t.Fatalf("%v: %v", id, err)
		}

		shardDisks[j], err = disk.NewDisk(id, dataDir)
		if err != nil {
			t.Fatalf("%v: %v", id, err)
		}
	}

	erasureDisk := NewErasure(shardDisks, count)

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				uploadID := disk.NewUploadID()

				if err = erasureDisk.InitUpload(uploadID); err != nil {
					t.Fatal(err)
				}

				for j := 0; j < len(testCase.parts); j++ {
					tempFilename := disk.NewTempFilename()
					info := &erasure.Info{
						DataCount:   4,
						ParityCount: 3,
						Size:        testCase.parts[j].Size,
						ShardSize:   MiB,
					}

					if _, err := erasureDisk.SaveTempFile(tempFilename, randReader(), true, info); err != nil {
						t.Fatalf("%v: %v", testCase.parts[j].ID, err)
					}

					if err := erasureDisk.UploadPart(uploadID, testCase.parts[j].ID, tempFilename); err != nil {
						t.Fatalf("%v: %v", testCase.parts[j].ID, err)
					}
				}

				if err := erasureDisk.AbortUpload(uploadID); err != nil {
					t.Fatal(err)
				}

				if err := erasureDisk.AbortUpload(uploadID); err == nil {
					t.Fatalf("mismatch: expected: <error>, got: <nil>")
				}
			},
		)
	}
}

func TestCompleteUpload(t *testing.T) {
	testCases := []struct {
		parts    []Part
		dataInfo *DataInfo
	}{
		{
			parts: []Part{newPart("1", 16279)},
			dataInfo: &DataInfo{
				Parts: []Part{
					{
						Info: erasure.Info{
							DataCount:   4,
							ParityCount: 3,
							Size:        16279,
							ShardSize:   1048576,
							ShardIDs:    []string{"d0", "d1", "d2", "d3", "d4", "d5", "d6"},
						},
						ID: "1",
					},
				},
				Size: 16279,
			},
		},
		{
			parts: []Part{newPart("3", 16279), newPart("8", 10992), newPart("1", 25489)},
			dataInfo: &DataInfo{
				Parts: []Part{
					{
						Info: erasure.Info{
							DataCount:   4,
							ParityCount: 3,
							Size:        16279,
							ShardSize:   1048576,
							ShardIDs:    []string{"d0", "d1", "d2", "d3", "d4", "d5", "d6"},
						},
						ID: "3",
					},
					{
						Info: erasure.Info{
							DataCount:   4,
							ParityCount: 3,
							Size:        10992,
							ShardSize:   1048576,
							ShardIDs:    []string{"d0", "d1", "d2", "d3", "d4", "d5", "d6"},
						},
						ID: "8",
					},
					{
						Info: erasure.Info{
							DataCount:   4,
							ParityCount: 3,
							Size:        25489,
							ShardSize:   1048576,
							ShardIDs:    []string{"d0", "d1", "d2", "d3", "d4", "d5", "d6"},
						},
						ID: "1",
					},
				},
				Size: 52760,
			},
		},
	}

	id := xrand.NewID(8).String()
	workDir := id
	if err := os.Mkdir(workDir, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	defer func() {
		os.RemoveAll(workDir)
	}()

	count := uint64(7)
	shardDisks := make([]*disk.Disk, count)
	var err error
	for j := uint64(0); j < count; j++ {
		id := fmt.Sprintf("d%v", j)
		dataDir := path.Join(workDir, id)
		if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
			t.Fatalf("%v: %v", id, err)
		}

		shardDisks[j], err = disk.NewDisk(id, dataDir)
		if err != nil {
			t.Fatalf("%v: %v", id, err)
		}
	}

	erasureDisk := NewErasure(shardDisks, count)

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				uploadID := disk.NewUploadID()

				if err = erasureDisk.InitUpload(uploadID); err != nil {
					t.Fatal(err)
				}

				for j := 0; j < len(testCase.parts); j++ {
					tempFilename := disk.NewTempFilename()
					info := &erasure.Info{
						DataCount:   4,
						ParityCount: 3,
						Size:        testCase.parts[j].Size,
						ShardSize:   MiB,
					}

					if _, err := erasureDisk.SaveTempFile(tempFilename, randReader(), true, info); err != nil {
						t.Fatalf("%v: %v", testCase.parts[j].ID, err)
					}

					if err := erasureDisk.UploadPart(uploadID, testCase.parts[j].ID, tempFilename); err != nil {
						t.Fatalf("%v: %v", testCase.parts[j].ID, err)
					}

					testCase.parts[j].Info = *info
				}

				dataID := disk.NewDataID()
				dataInfo, err := erasureDisk.CompleteUpload(dataID, uploadID, testCase.parts)
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(dataInfo, testCase.dataInfo) {
					t.Fatalf("mismatch: dataInfo: expected: %+v, got: %+v", testCase.dataInfo, dataInfo)
				}
			},
		)
	}
}

func TestGet(t *testing.T) {
	testCases := []struct {
		parts    []Part
		offset   int64
		length   uint64
		checksum string
	}{
		{[]Part{newPart("1", 16279)}, 0, 10, "cb681256c303aaacfc24ed94cb5ffd6a84fcde8a6721213b0a757ba40ac4a4a9"},
		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 0, 10, "cb681256c303aaacfc24ed94cb5ffd6a84fcde8a6721213b0a757ba40ac4a4a9"},
		{[]Part{newPart("1", 16279)}, 10, 7, "aa88fcc3f216be54199c57fd835b9921a6fd259edc834d115b6b898ccfaa4c25"},
		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 10, 7, "aa88fcc3f216be54199c57fd835b9921a6fd259edc834d115b6b898ccfaa4c25"},
		{[]Part{newPart("1", 16279)}, 0, 16279, "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"},
		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 16279, 10992, "60c7436deea126319878ecbf43b853f39f3451dccacde02b4e6a66082e9d168a"},
		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 12958, 10992, "6b3559a522b87e0a9bbe0b74ace31a83dedc0d46f3eee2b1aea0b88fb312f883"},
		{[]Part{newPart("3", 16279), newPart("8", 10992), newPart("1", 25489)}, 12958, 17343, "f76f77b058eb962e5099062cccfcf5e9363cdb533a86563680f8488ee99f0cfa"},
		{[]Part{newPart("3", 16279), newPart("8", 10992), newPart("1", 25489)}, 27271, 70, "f9b63a4a399ca9f26b15f7dc5987b1644b6054ac9c34f6c136c6576eb77d9956"},
	}

	id := xrand.NewID(8).String()
	workDir := id
	if err := os.Mkdir(workDir, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	defer func() {
		os.RemoveAll(workDir)
	}()

	count := uint64(7)
	shardDisks := make([]*disk.Disk, count)
	var err error
	for j := uint64(0); j < count; j++ {
		id := fmt.Sprintf("d%v", j)
		dataDir := path.Join(workDir, id)
		if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
			t.Fatalf("%v: %v", id, err)
		}

		shardDisks[j], err = disk.NewDisk(id, dataDir)
		if err != nil {
			t.Fatalf("%v: %v", id, err)
		}
	}

	erasureDisk := NewErasure(shardDisks, count)

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				uploadID := disk.NewUploadID()

				if err = erasureDisk.InitUpload(uploadID); err != nil {
					t.Fatal(err)
				}

				for j := 0; j < len(testCase.parts); j++ {
					tempFilename := disk.NewTempFilename()
					info := &erasure.Info{
						DataCount:   4,
						ParityCount: 3,
						Size:        testCase.parts[j].Size,
						ShardSize:   MiB,
					}

					if _, err := erasureDisk.SaveTempFile(tempFilename, randReader(), true, info); err != nil {
						t.Fatalf("%v: %v", testCase.parts[j].ID, err)
					}

					if err := erasureDisk.UploadPart(uploadID, testCase.parts[j].ID, tempFilename); err != nil {
						t.Fatalf("%v: %v", testCase.parts[j].ID, err)
					}

					testCase.parts[j].Info = *info
				}

				dataID := disk.NewDataID()
				dataInfo, err := erasureDisk.CompleteUpload(dataID, uploadID, testCase.parts)
				if err != nil {
					t.Fatal(err)
				}

				rc, err := erasureDisk.Get(dataID, dataInfo, testCase.offset, testCase.length)
				if err != nil {
					t.Fatal(err)
				}

				defer rc.Close()

				hasher := xhash.MustGetNewHash(xhash.HighwayHash256Algorithm, nil)
				length, err := io.Copy(hasher, rc)
				if err != nil {
					t.Fatal(err)
				}

				if testCase.length != uint64(length) {
					t.Fatalf("expected: %v, got: %v", testCase.length, length)
				}

				checksum := hasher.HexSum(nil)

				if testCase.checksum != checksum {
					t.Fatalf("expected: %v, got: %v", testCase.checksum, checksum)
				}
			},
		)
	}
}
