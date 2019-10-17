package erasure

import (
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/balamurugana/goat/pkg/erasure"
)

func randReader() io.Reader {
	return rand.New(rand.NewSource(271828))
}

func newPart(id string, size uint64) Part {
	return Part{
		ID: id,
		Info: erasure.Info{
			Size: size,
		},
	}
}

func TestDataInfoGetParts(t *testing.T) {
	testCases := []struct {
		parts         []Part
		offset        int64
		length        int64
		requiredParts []Part
		bytesToSkip   int64
		bytesToRead   int64
	}{
		{[]Part{newPart("1", 16279)}, 0, 10, []Part{newPart("1", 16279)}, 0, 10},
		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 0, 10, []Part{newPart("3", 16279)}, 0, 10},
		{[]Part{newPart("1", 16279)}, 10, 7, []Part{newPart("1", 16279)}, 10, 7},
		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 10, 7, []Part{newPart("3", 16279)}, 10, 7},
		{[]Part{newPart("1", 16279)}, 0, 16279, []Part{newPart("1", 16279)}, 0, 16279},
		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 16279, 10992, []Part{newPart("8", 10992)}, 0, 10992},
		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 12958, 10992, []Part{newPart("3", 16279), newPart("8", 10992)}, 12958, 7671},
		{[]Part{newPart("3", 16279), newPart("8", 10992), newPart("1", 25489)}, 12958, 17343, []Part{newPart("3", 16279), newPart("8", 10992), newPart("1", 25489)}, 12958, 3030},
		{[]Part{newPart("3", 16279), newPart("8", 10992), newPart("1", 25489)}, 27271, 70, []Part{newPart("1", 25489)}, 0, 70},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				dataInfo := DataInfo{Parts: testCase.parts}
				requiredParts, bytesToSkip, bytesToRead := dataInfo.getParts(testCase.offset, testCase.length)

				if !reflect.DeepEqual(testCase.requiredParts, requiredParts) {
					t.Fatalf("mismatch: requiredParts: expected: %+v, got: %+v", testCase.requiredParts, requiredParts)
				}

				if testCase.bytesToSkip != bytesToSkip {
					t.Fatalf("mismatch: bytesToSkip: expected: %v, got: %v", testCase.bytesToSkip, bytesToSkip)
				}

				if testCase.bytesToRead != bytesToRead {
					t.Fatalf("mismatch: bytesToRead: expected: %v, got: %v", testCase.bytesToRead, bytesToRead)
				}
			},
		)
	}
}

// func TestDataReader(t *testing.T) {
// 	testCases := []struct {
// 		parts    []Part
// 		offset   int64
// 		length   uint64
// 		checksum string
// 	}{
// 		{[]Part{newPart("1", 16279)}, 0, 10, "cb681256c303aaacfc24ed94cb5ffd6a84fcde8a6721213b0a757ba40ac4a4a9"},
// 		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 0, 10, "cb681256c303aaacfc24ed94cb5ffd6a84fcde8a6721213b0a757ba40ac4a4a9"},
// 		{[]Part{newPart("1", 16279)}, 10, 7, "aa88fcc3f216be54199c57fd835b9921a6fd259edc834d115b6b898ccfaa4c25"},
// 		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 10, 7, "aa88fcc3f216be54199c57fd835b9921a6fd259edc834d115b6b898ccfaa4c25"},
// 		{[]Part{newPart("1", 16279)}, 0, 16279, "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b"},
// 		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 16279, 10992, "60c7436deea126319878ecbf43b853f39f3451dccacde02b4e6a66082e9d168a"},
// 		{[]Part{newPart("3", 16279), newPart("8", 10992)}, 12958, 10992, "6b3559a522b87e0a9bbe0b74ace31a83dedc0d46f3eee2b1aea0b88fb312f883"},
// 		{[]Part{newPart("3", 16279), newPart("8", 10992), newPart("1", 25489)}, 12958, 17343, "f76f77b058eb962e5099062cccfcf5e9363cdb533a86563680f8488ee99f0cfa"},
// 		{[]Part{newPart("3", 16279), newPart("8", 10992), newPart("1", 25489)}, 27271, 70, "f9b63a4a399ca9f26b15f7dc5987b1644b6054ac9c34f6c136c6576eb77d9956"},
// 	}
//
// 	for i, testCase := range testCases {
// 		t.Run(
// 			fmt.Sprintf("test%v", i),
// 			func(t *testing.T) {
// 				dataDir := xrand.NewID(8).String()
// 				if err := os.Mkdir(dataDir, os.ModePerm); err != nil {
// 					t.Fatal(err)
// 				}
//
// 				defer func() {
// 					os.RemoveAll(dataDir)
// 				}()
//
// 				size := uint64(0)
// 				for _, part := range testCase.parts {
// 					filename := path.Join(dataDir, part.ID+".part")
// 					if _, err := xos.WriteFile(filename, randReader(), part.Size, true); err != nil {
// 						t.Fatalf("%+v: %v", part, err)
// 					}
//
// 					size += part.Size
// 				}
//
// 				dataInfo := &DataInfo{
// 					Parts: testCase.parts,
// 					Size:  size,
// 				}
//
// 				dataJSONFile := path.Join(dataDir, "data.json")
// 				file, err := os.Create(dataJSONFile)
// 				if err != nil {
// 					t.Fatal(err)
// 				}
//
// 				if err = json.NewEncoder(file).Encode(dataInfo); err != nil {
// 					file.Close()
// 					t.Fatal(err)
// 				}
// 				file.Close()
//
// 				dr, err := newDataReader(dataDir, dataInfo, testCase.offset, testCase.length)
// 				if err != nil {
// 					t.Fatal(err)
// 				}
//
// 				defer dr.Close()
//
// 				hasher := xhash.MustGetNewHash(xhash.HighwayHash256Algorithm, nil)
// 				length, err := io.Copy(hasher, dr)
// 				if err != nil {
// 					t.Fatal(err)
// 				}
//
// 				if testCase.length != uint64(length) {
// 					t.Fatalf("expected: %v, got: %v", testCase.length, length)
// 				}
//
// 				checksum := hasher.HexSum(nil)
//
// 				if testCase.checksum != checksum {
// 					t.Fatalf("expected: %v, got: %v", testCase.checksum, checksum)
// 				}
// 			},
// 		)
// 	}
// }
