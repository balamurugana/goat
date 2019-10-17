package disk

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"reflect"
	"testing"

	xos "github.com/balamurugana/goat/pkg/os"
	xrand "github.com/balamurugana/goat/pkg/rand"
)

func populate(rootDir string, count, maxDepth int) {
	rand := rand.New(rand.NewSource(271828))
	for c := 0; c < count; c++ {
		l := 2*rand.Intn(maxDepth) + 1
		b := make([]byte, l)
		for i := 0; i < l; i++ {
			b[i] = byte(97 + rand.Intn(26))

			if i < l-1 {
				i++
				b[i] = '/'
			}
		}

		ioutil.WriteFile(objectID, []byte{}, 0644)
		xos.CreatePath(path.Join(rootDir, string(b), objectID), objectID, false)
		if l == maxDepth {
			ioutil.WriteFile(slashObjectID, []byte{}, 0644)
			xos.CreatePath(path.Join(rootDir, string(b), slashObjectID), slashObjectID, false)
		}
	}

	ioutil.WriteFile(slashObjectID, []byte{}, 0644)
	xos.CreatePath(path.Join(rootDir, "c", slashObjectID), slashObjectID, false)
}

func testListObjects(t *testing.T, rootDir string) {
	testCases := []struct {
		prefix     string
		startAfter string
		maxKeys    int

		isTruncated bool
		nextMarker  string
		objects     []string
		prefixes    []string
	}{
		// case 0
		{"", "", 1000, false, "", []string{"a", "b", "c", "d", "e", "f", "g", "h", "j", "k", "l", "m", "n", "p", "q", "r", "t", "u", "v", "w", "x", "y"}, []string{"a/", "b/", "c/", "d/", "e/", "f/", "g/", "h/", "j/", "k/", "l/", "m/", "n/", "o/", "p/", "q/", "r/", "s/", "t/", "u/", "v/", "w/", "x/", "y/", "z/"}},
		{"", "", 20, true, "k/", []string{"a", "b", "c", "d", "e", "f", "g", "h", "j", "k"}, []string{"a/", "b/", "c/", "d/", "e/", "f/", "g/", "h/", "j/", "k/"}},
		// case 2
		{"c", "", 20, false, "", []string{"c"}, []string{"c/"}},
		{"c/", "", 20, false, "", []string{"c/", "c/r", "c/s"}, []string{"c/r/", "c/s/", "c/w/", "c/y/"}},
		{"s", "", 20, false, "", nil, []string{"s/"}},
		{"s/", "", 20, false, "", []string{"s/h", "s/k", "s/s", "s/t", "s/v"}, []string{"s/h/", "s/k/", "s/s/", "s/t/", "s/v/"}},
		{"s/c", "", 20, false, "", nil, nil},
		{"s/h", "", 20, false, "", []string{"s/h"}, []string{"s/h/"}},
		{"s/h/", "", 20, false, "", []string{"s/h/", "s/h/y"}, nil},
		// case 9
		{"", "c", 20, true, "n", []string{"d", "e", "f", "g", "h", "j", "k", "l", "m", "n"}, []string{"c/", "d/", "e/", "f/", "g/", "h/", "j/", "k/", "l/", "m/"}},
		{"", "c/", 20, true, "n/", []string{"d", "e", "f", "g", "h", "j", "k", "l", "m", "n"}, []string{"d/", "e/", "f/", "g/", "h/", "j/", "k/", "l/", "m/", "n/"}},
		{"", "s", 20, false, "", []string{"t", "u", "v", "w", "x", "y"}, []string{"s/", "t/", "u/", "v/", "w/", "x/", "y/", "z/"}},
		{"", "s/", 20, false, "", []string{"t", "u", "v", "w", "x", "y"}, []string{"t/", "u/", "v/", "w/", "x/", "y/", "z/"}},
		{"", "s/c", 20, false, "", []string{"t", "u", "v", "w", "x", "y"}, []string{"t/", "u/", "v/", "w/", "x/", "y/", "z/"}},
		{"", "s/h", 20, false, "", []string{"t", "u", "v", "w", "x", "y"}, []string{"t/", "u/", "v/", "w/", "x/", "y/", "z/"}},
		{"", "s/h/", 20, false, "", []string{"t", "u", "v", "w", "x", "y"}, []string{"t/", "u/", "v/", "w/", "x/", "y/", "z/"}},
		// case 16
		{"s", "s/h", 20, false, "", nil, nil},
		{"s/", "s/h", 20, false, "", []string{"s/k", "s/s", "s/t", "s/v"}, []string{"s/h/", "s/k/", "s/s/", "s/t/", "s/v/"}},
		{"s/", "s/c", 20, false, "", []string{"s/h", "s/k", "s/s", "s/t", "s/v"}, []string{"s/h/", "s/k/", "s/s/", "s/t/", "s/v/"}},
		{"s", "s", 20, false, "", nil, []string{"s/"}},
		{"s/h", "s/h", 20, false, "", nil, []string{"s/h/"}},
		{"s/h", "s/h/", 20, false, "", nil, nil},
		{"s/h/", "s/h", 20, false, "", []string{"s/h/", "s/h/y"}, nil},
		{"s/h/", "s/h/", 20, false, "", []string{"s/h/y"}, nil},
		{"aa/", "", 20, false, "", nil, nil},  // prefix does not exist.
		{"a/bb", "", 20, false, "", nil, nil}, // prefix does not exist.
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("case%v", i),
			func(t *testing.T) {
				objects, prefixes, isTruncated, nextMarker, err := ListObjects(rootDir, testCase.prefix, testCase.startAfter, testCase.maxKeys, false)
				if err != nil {
					t.Fatal(err)
				}

				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(objects, testCase.objects) {
					t.Fatalf("objects: expected: %v, got: %v", testCase.objects, objects)
				}
				if !reflect.DeepEqual(prefixes, testCase.prefixes) {
					t.Fatalf("prefixes: expected: %v, got: %v", testCase.prefixes, prefixes)
				}
				if isTruncated != testCase.isTruncated {
					t.Fatalf("isTruncated: expected: %v, got: %v", testCase.isTruncated, isTruncated)
				}
				if nextMarker != testCase.nextMarker {
					t.Fatalf("nextMarker: expected: %v, got: %v", testCase.nextMarker, nextMarker)
				}
			},
		)
	}
}

func testListDirRecursive(t *testing.T, rootDir string) {
	testCases := []struct {
		prefix     string
		startAfter string
		maxKeys    int

		isTruncated bool
		objects     []string
	}{
		{"", "", 20, true, []string{"a", "a/b", "a/b/", "a/e/u", "a/s", "a/s/", "a/z/g", "b", "b/a", "b/a/", "b/m", "b/m/", "b/n", "b/n/", "b/p", "b/p/", "b/w/y", "c", "c/", "c/r"}},
		// case 1
		{"c", "", 20, false, []string{"c", "c/", "c/r", "c/r/", "c/s", "c/s/", "c/w/p", "c/y/m"}},
		{"s", "", 20, false, []string{"s/h", "s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/"}},
		{"s/", "", 20, false, []string{"s/h", "s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/"}},
		{"s/c", "", 20, false, nil},
		{"s/h", "", 20, false, []string{"s/h", "s/h/", "s/h/y"}},
		{"s/h/", "", 20, false, []string{"s/h/", "s/h/y"}},
		{"c/", "", 20, false, []string{"c/", "c/r", "c/r/", "c/s", "c/s/", "c/w/p", "c/y/m"}},
		// case 8
		{"", "c", 20, true, []string{"c/", "c/r", "c/r/", "c/s", "c/s/", "c/w/p", "c/y/m", "d", "d/k/c", "e", "e/e/c", "e/g", "e/g/", "e/m", "e/m/", "f", "f/c/e", "f/e/p", "f/x", "f/x/"}},
		{"", "s", 20, true, []string{"s/h", "s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/", "t", "t/u/l", "t/x/i", "t/x/r", "t/z/t", "u", "u/d/i", "u/u/i", "v"}},
		{"", "s/", 20, true, []string{"s/h", "s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/", "t", "t/u/l", "t/x/i", "t/x/r", "t/z/t", "u", "u/d/i", "u/u/i", "v"}},
		{"", "s/c", 20, true, []string{"s/h", "s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/", "t", "t/u/l", "t/x/i", "t/x/r", "t/z/t", "u", "u/d/i", "u/u/i", "v"}},
		{"", "s/h", 20, true, []string{"s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/", "t", "t/u/l", "t/x/i", "t/x/r", "t/z/t", "u", "u/d/i", "u/u/i", "v", "v/x/o"}},
		{"", "s/h/", 20, true, []string{"s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/", "t", "t/u/l", "t/x/i", "t/x/r", "t/z/t", "u", "u/d/i", "u/u/i", "v", "v/x/o", "w"}},
		{"", "c/", 20, true, []string{"c/r", "c/r/", "c/s", "c/s/", "c/w/p", "c/y/m", "d", "d/k/c", "e", "e/e/c", "e/g", "e/g/", "e/m", "e/m/", "f", "f/c/e", "f/e/p", "f/x", "f/x/", "g"}},
		// case 15
		{"s", "s/h", 20, false, []string{"s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/"}},
		{"s/", "s/h", 20, false, []string{"s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/"}},
		{"s/", "s/c", 20, false, []string{"s/h", "s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/"}},
		{"s", "s", 20, false, []string{"s/h", "s/h/", "s/h/y", "s/k", "s/k/", "s/s", "s/s/", "s/t", "s/t/", "s/v", "s/v/"}},
		{"s/h", "s/h", 20, false, []string{"s/h/", "s/h/y"}},
		{"s/h", "s/h/", 20, false, []string{"s/h/y"}},
		{"s/h/", "s/h", 20, false, []string{"s/h/", "s/h/y"}},
		{"s/h/", "s/h/", 20, false, []string{"s/h/y"}},
		{"aa/", "", 20, false, nil},  // prefix does not exist.
		{"a/bb", "", 20, false, nil}, // prefix does not exist.
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("case%v", i),
			func(t *testing.T) {
				objects, _, isTruncated, _, err := ListObjects(rootDir, testCase.prefix, testCase.startAfter, testCase.maxKeys, true)
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(objects, testCase.objects) {
					t.Fatalf("objects: expected: %v, got: %v", testCase.objects, objects)
				}
				if isTruncated != testCase.isTruncated {
					t.Fatalf("isTruncated: expected: %v, got: %v", testCase.isTruncated, isTruncated)
				}
			},
		)
	}
}

func TestListObjects(t *testing.T) {
	count := 100
	maxDepth := 3
	rootDir := xrand.NewID(8).String()
	populate(rootDir, count, maxDepth)
	defer func() {
		os.RemoveAll(rootDir)
	}()

	t.Run("recursive=false", func(t *testing.T) { testListObjects(t, rootDir) })
	t.Run("recursive=true", func(t *testing.T) { testListDirRecursive(t, rootDir) })
}
