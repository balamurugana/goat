package os

import (
	"os"
	"reflect"
	"testing"

	xrand "github.com/balamurugana/goat/pkg/rand"
)

func TestChecksumFile(t *testing.T) {
	filename := xrand.NewID(8).String()
	size := uint64(26)
	blockSize := uint(4)
	blockCount := uint(7)

	file, err := createChecksumFile(filename, blockSize, blockCount, size)
	if err != nil {
		t.Fatalf("failed to create file %v: %v", filename, err)
	}

	defer func() {
		file.Close()
		os.Remove(filename + ".checksum")
	}()

	if _, err = file.Write([]byte("Sphi")); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("nx o")); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("f bl")); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("ack ")); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("quar")); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("tz, ")); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte("my")); err != nil {
		t.Fatal(err)
	}

	file.Close()

	file, err = openChecksumFile(filename)
	if err != nil {
		t.Fatalf("failed to open file %v: %v", filename, err)
	}

	expectedheader := &checksumHeader{
		HashName:   "HighwayHash256",
		HashKey:    "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		HashLength: 64,
		BlockSize:  4,
		BlockCount: 7,
		DataLength: 26,
	}

	if !reflect.DeepEqual(expectedheader, file.header) {
		t.Fatalf("header mismatch. expected: %+v, got: %+v", expectedheader, file.header)
	}

	checksums := []string{
		"bab5321b3a3de8b4e71aed8faa7c863937743b70d2c365cfe7ef3525a5c18e06",
		"62a7f60f9c2ed9acda82c7716d2533db045e3a8d6d5b6e03cab9ef9827accf1a",
		"e7c1a146d54fd3577675697eaf950ef9352a9a502a7f716336d3945d5a9c0fc3",
		"9f95aafd7ac31a6ab4266bc351c795a50a2f86c1290d5162b4b583eab6916353",
		"9e5c1f4326420e849f7014e2d3cd63a2f415d3f9af31c0c30df58fdd26c9b4fb",
		"4cd0a8f7b517063018516489f4f6e23a628cf0dff8ea8ffdd7a29a98ae4a01f4",
		"d6fe5f40a3537f76b1d6d3211d43b7df4f477285f4f421c4a5b4443630495447",
	}

	for i, expected := range checksums {
		got, err := file.ReadSum()
		if err != nil {
			t.Fatal(err)
		}

		if expected != got {
			t.Fatalf("block %v; checksum mismatch. expected: %+v, got: %+v", i+1, expected, got)
		}
	}
}
