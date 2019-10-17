package disk

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"testing"

	xhash "github.com/balamurugana/goat/pkg/hash"
	xrand "github.com/balamurugana/goat/pkg/rand"
)

func TestSaveTempFile(t *testing.T) {
	t.Run(
		"test0",
		func(t *testing.T) {
			id := xrand.NewID(8).String()
			dataDir := id
			if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(dataDir)
			}()

			disk, err := NewDisk(id, dataDir)
			if err != nil {
				t.Fatal(err)
			}

			tempFilename := NewTempFilename()
			expectedChecksum := "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"
			checksum, err := disk.SaveTempFile(tempFilename, randReader(), 16279, true)
			if err != nil {
				t.Fatal(err)
			}

			if checksum != expectedChecksum {
				t.Fatalf("mismatch: checksum: expected: %v, got: %v", expectedChecksum, checksum)
			}
		},
	)
}

func TestRemoveTempFile(t *testing.T) {
	testCases := []struct {
		size     uint64
		checksum string
	}{
		{16279, "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				id := xrand.NewID(8).String()
				dataDir := id
				if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
					t.Fatal(err)
				}

				defer func() {
					os.RemoveAll(dataDir)
				}()

				disk, err := NewDisk(id, dataDir)
				if err != nil {
					t.Fatal(err)
				}

				tempFilename := NewTempFilename()
				checksum, err := disk.SaveTempFile(tempFilename, randReader(), testCase.size, true)
				if err != nil {
					t.Fatal(err)
				}

				if checksum != testCase.checksum {
					t.Fatalf("mismatch: checksum: expected: %v, got: %v", testCase.checksum, checksum)
				}

				if err = disk.RemoveTempFile(tempFilename, true); err != nil {
					t.Fatal(err)
				}

				if err = disk.RemoveTempFile(tempFilename, true); err == nil {
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
			dataDir := id
			if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(dataDir)
			}()

			disk, err := NewDisk(id, dataDir)
			if err != nil {
				t.Fatal(err)
			}

			uploadID := NewUploadID()
			if err = disk.InitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err = disk.InitUpload(uploadID); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}
		},
	)
}

func TestRevertInitUpload(t *testing.T) {
	t.Run(
		"test0",
		func(t *testing.T) {
			id := xrand.NewID(8).String()
			dataDir := id
			if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(dataDir)
			}()

			disk, err := NewDisk(id, dataDir)
			if err != nil {
				t.Fatal(err)
			}

			uploadID := NewUploadID()
			if err = disk.InitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err = disk.RevertInitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err = disk.RevertInitUpload(uploadID); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}
		},
	)
}

func TestUploadPart(t *testing.T) {
	t.Run(
		"test0",
		func(t *testing.T) {
			id := xrand.NewID(8).String()
			dataDir := id
			if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(dataDir)
			}()

			disk, err := NewDisk(id, dataDir)
			if err != nil {
				t.Fatal(err)
			}

			expectedChecksum := "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"
			tempFilename := NewTempFilename()
			checksum, err := disk.SaveTempFile(tempFilename, randReader(), 16279, true)
			if err != nil {
				t.Fatal(err)
			}

			if checksum != expectedChecksum {
				t.Fatalf("mismatch: checksum: expected: %v, got: %v", expectedChecksum, checksum)
			}

			uploadID := NewUploadID()

			if err := disk.UploadPart(uploadID, "123", tempFilename); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}

			if err = disk.InitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err := disk.UploadPart(uploadID, "123", tempFilename); err != nil {
				t.Fatal(err)
			}

			if err := disk.UploadPart(uploadID, "213", tempFilename); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}
		},
	)
}

func TestRevertUploadPart(t *testing.T) {
	t.Run(
		"test0",
		func(t *testing.T) {
			id := xrand.NewID(8).String()
			dataDir := id
			if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(dataDir)
			}()

			disk, err := NewDisk(id, dataDir)
			if err != nil {
				t.Fatal(err)
			}

			expectedChecksum := "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"
			tempFilename := NewTempFilename()
			checksum, err := disk.SaveTempFile(tempFilename, randReader(), 16279, true)
			if err != nil {
				t.Fatal(err)
			}

			if checksum != expectedChecksum {
				t.Fatalf("mismatch: checksum: expected: %v, got: %v", expectedChecksum, checksum)
			}

			uploadID := NewUploadID()

			if err := disk.RevertUploadPart(uploadID, "123", tempFilename); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}

			if err = disk.InitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err := disk.UploadPart(uploadID, "123", tempFilename); err != nil {
				t.Fatal(err)
			}

			if err := disk.RevertUploadPart(uploadID, "123", tempFilename); err != nil {
				t.Fatal(err)
			}

			if err := disk.RevertUploadPart(uploadID, "123", tempFilename); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}
		},
	)
}

func TestAbortUpload(t *testing.T) {
	t.Run(
		"test0",
		func(t *testing.T) {
			id := xrand.NewID(8).String()
			dataDir := id
			if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(dataDir)
			}()

			disk, err := NewDisk(id, dataDir)
			if err != nil {
				t.Fatal(err)
			}

			expectedChecksum := "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"
			tempFilename := NewTempFilename()
			checksum, err := disk.SaveTempFile(tempFilename, randReader(), 16279, true)
			if err != nil {
				t.Fatal(err)
			}

			if checksum != expectedChecksum {
				t.Fatalf("mismatch: checksum: expected: %v, got: %v", expectedChecksum, checksum)
			}

			uploadID := NewUploadID()

			if err = disk.AbortUpload(uploadID); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}

			if err = disk.InitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err := disk.UploadPart(uploadID, "123", tempFilename); err != nil {
				t.Fatal(err)
			}

			if err = disk.AbortUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err = disk.AbortUpload(uploadID); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}
		},
	)
}

func TestRevertAbortUpload(t *testing.T) {
	t.Run(
		"test0",
		func(t *testing.T) {
			id := xrand.NewID(8).String()
			dataDir := id
			if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(dataDir)
			}()

			disk, err := NewDisk(id, dataDir)
			if err != nil {
				t.Fatal(err)
			}

			expectedChecksum := "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"
			tempFilename := NewTempFilename()
			checksum, err := disk.SaveTempFile(tempFilename, randReader(), 16279, true)
			if err != nil {
				t.Fatal(err)
			}

			if checksum != expectedChecksum {
				t.Fatalf("mismatch: checksum: expected: %v, got: %v", expectedChecksum, checksum)
			}

			uploadID := NewUploadID()

			if err = disk.InitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err := disk.UploadPart(uploadID, "123", tempFilename); err != nil {
				t.Fatal(err)
			}

			if err = disk.AbortUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err = disk.RevertAbortUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			if err = disk.RevertAbortUpload(uploadID); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}
		},
	)
}

func TestCompleteUpload(t *testing.T) {
	t.Run(
		"test0",
		func(t *testing.T) {
			id := xrand.NewID(8).String()
			dataDir := id
			if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(dataDir)
			}()

			disk, err := NewDisk(id, dataDir)
			if err != nil {
				t.Fatal(err)
			}

			uploadID := NewUploadID()

			if err = disk.InitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			expectedChecksum := "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"
			tempFilename := NewTempFilename()
			checksum, err := disk.SaveTempFile(tempFilename, randReader(), 16279, true)
			if err != nil {
				t.Fatal(err)
			}

			if checksum != expectedChecksum {
				t.Fatalf("mismatch: checksum: expected: %v, got: %v", expectedChecksum, checksum)
			}

			if err := disk.UploadPart(uploadID, "3", tempFilename); err != nil {
				t.Fatal(err)
			}

			expectedChecksum = "0232a959153a87b0f59d8491b68eff93f1b62596c96cf2c3cfde7ec52457e64d"
			tempFilename = NewTempFilename()
			checksum, err = disk.SaveTempFile(tempFilename, randReader(), 70009289, true)
			if err != nil {
				t.Fatal(err)
			}

			if checksum != expectedChecksum {
				t.Fatalf("mismatch: checksum: expected: %v, got: %v", expectedChecksum, checksum)
			}

			if err := disk.UploadPart(uploadID, "8", tempFilename); err != nil {
				t.Fatal(err)
			}

			dataID := NewDataID()
			parts := []Part{{"3", 16279}, {"8", 70009289}}
			if err = disk.CompleteUpload(dataID, uploadID, parts); err != nil {
				t.Fatal(err)
			}

			expectedDataInfo := DataInfo{
				Parts: parts,
				Size:  16279 + 70009289,
			}

			dataJSONFile := path.Join(dataDir, "data", dataID.String(), "data.json")
			file, err := os.Open(dataJSONFile)
			if err != nil {
				t.Fatal(err)
			}

			defer file.Close()

			var dataInfo DataInfo
			if err = json.NewDecoder(file).Decode(&dataInfo); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(dataInfo, expectedDataInfo) {
				t.Fatalf("mismatch: DataInfo: expected: %+v, got: %+v", expectedDataInfo, dataInfo)
			}
		},
	)
}

func TestRevertCompleteUpload(t *testing.T) {
	t.Run(
		"test0",
		func(t *testing.T) {
			id := xrand.NewID(8).String()
			dataDir := id
			if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
				t.Fatal(err)
			}

			defer func() {
				os.RemoveAll(dataDir)
			}()

			disk, err := NewDisk(id, dataDir)
			if err != nil {
				t.Fatal(err)
			}

			uploadID := NewUploadID()

			if err = disk.InitUpload(uploadID); err != nil {
				t.Fatal(err)
			}

			expectedChecksum := "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"
			tempFilename := NewTempFilename()
			checksum, err := disk.SaveTempFile(tempFilename, randReader(), 16279, true)
			if err != nil {
				t.Fatal(err)
			}

			if checksum != expectedChecksum {
				t.Fatalf("mismatch: checksum: expected: %v, got: %v", expectedChecksum, checksum)
			}

			if err := disk.UploadPart(uploadID, "3", tempFilename); err != nil {
				t.Fatal(err)
			}

			expectedChecksum = "0232a959153a87b0f59d8491b68eff93f1b62596c96cf2c3cfde7ec52457e64d"
			tempFilename = NewTempFilename()
			checksum, err = disk.SaveTempFile(tempFilename, randReader(), 70009289, true)
			if err != nil {
				t.Fatal(err)
			}

			if checksum != expectedChecksum {
				t.Fatalf("mismatch: checksum: expected: %v, got: %v", expectedChecksum, checksum)
			}

			if err := disk.UploadPart(uploadID, "8", tempFilename); err != nil {
				t.Fatal(err)
			}

			dataID := NewDataID()
			parts := []Part{{"3", 16279}, {"8", 70009289}}
			if err = disk.CompleteUpload(dataID, uploadID, parts); err != nil {
				t.Fatal(err)
			}

			expectedDataInfo := DataInfo{
				Parts: parts,
				Size:  16279 + 70009289,
			}

			dataJSONFile := path.Join(dataDir, "data", dataID.String(), "data.json")
			file, err := os.Open(dataJSONFile)
			if err != nil {
				t.Fatal(err)
			}

			defer file.Close()

			var dataInfo DataInfo
			if err = json.NewDecoder(file).Decode(&dataInfo); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(dataInfo, expectedDataInfo) {
				t.Fatalf("mismatch: DataInfo: expected: %+v, got: %+v", expectedDataInfo, dataInfo)
			}

			if err = disk.RevertCompleteUpload(dataID, uploadID, parts); err != nil {
				t.Fatal(err)
			}

			if err = disk.RevertCompleteUpload(dataID, uploadID, parts); err == nil {
				t.Fatalf("mismatch: expected: <error>, got: <nil>")
			}
		},
	)
}

func TestGet(t *testing.T) {
	testCases := []struct {
		parts    []Part
		offset   int64
		length   uint64
		checksum string
	}{
		{[]Part{{"1", 16279}}, 0, 10, "cb681256c303aaacfc24ed94cb5ffd6a84fcde8a6721213b0a757ba40ac4a4a9"},
		{[]Part{{"3", 16279}, {"8", 10992}}, 0, 10, "cb681256c303aaacfc24ed94cb5ffd6a84fcde8a6721213b0a757ba40ac4a4a9"},
		{[]Part{{"1", 16279}}, 10, 7, "aa88fcc3f216be54199c57fd835b9921a6fd259edc834d115b6b898ccfaa4c25"},
		{[]Part{{"3", 16279}, {"8", 10992}}, 10, 7, "aa88fcc3f216be54199c57fd835b9921a6fd259edc834d115b6b898ccfaa4c25"},
		{[]Part{{"1", 16279}}, 0, 16279, "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"},
		{[]Part{{"3", 16279}, {"8", 10992}}, 16279, 10992, "60c7436deea126319878ecbf43b853f39f3451dccacde02b4e6a66082e9d168a"},
		{[]Part{{"3", 16279}, {"8", 10992}}, 12958, 10992, "6b3559a522b87e0a9bbe0b74ace31a83dedc0d46f3eee2b1aea0b88fb312f883"},
		{[]Part{{"3", 16279}, {"8", 10992}, {"1", 25489}}, 12958, 17343, "f76f77b058eb962e5099062cccfcf5e9363cdb533a86563680f8488ee99f0cfa"},
		{[]Part{{"3", 16279}, {"8", 10992}, {"1", 25489}}, 27271, 70, "f9b63a4a399ca9f26b15f7dc5987b1644b6054ac9c34f6c136c6576eb77d9956"},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				id := xrand.NewID(8).String()
				dataDir := id
				if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
					t.Fatal(err)
				}

				defer func() {
					os.RemoveAll(dataDir)
				}()

				disk, err := NewDisk(id, dataDir)
				if err != nil {
					t.Fatal(err)
				}

				uploadID := NewUploadID()

				if err = disk.InitUpload(uploadID); err != nil {
					t.Fatal(err)
				}

				for j, part := range testCase.parts {
					tempFilename := NewTempFilename()
					if _, err := disk.SaveTempFile(tempFilename, randReader(), part.Size, true); err != nil {
						t.Fatalf("parts[%+v]: %v", j, err)
					}

					if err := disk.UploadPart(uploadID, part.ID, tempFilename); err != nil {
						t.Fatalf("parts[%+v]: %v", j, err)
					}
				}

				dataID := NewDataID()
				if err = disk.CompleteUpload(dataID, uploadID, testCase.parts); err != nil {
					t.Fatal(err)
				}

				rc, err := disk.Get(dataID, testCase.offset, testCase.length)
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
