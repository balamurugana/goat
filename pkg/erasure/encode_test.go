package erasure

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"reflect"
	"sync"
	"testing"

	xrand "github.com/balamurugana/goat/pkg/rand"
)

const MiB = 1 * 1024 * 1024

func randReader() io.Reader {
	return rand.New(rand.NewSource(271828))
}

func testWrite(t *testing.T, info *Info, dirname string) (shardChecksums []string, checksum string) {
	err := os.Mkdir(dirname, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}

	length := info.DataCount + info.ParityCount

	info.ShardIDs = make([]string, length)
	for i := range info.ShardIDs {
		info.ShardIDs[i] = path.Join(dirname, fmt.Sprintf("shard.%v", i))
	}

	files := map[string]*os.File{}
	filesMutex := sync.Mutex{}
	getShardWriter := func(shardID string) (io.Writer, error) {
		file, err := os.Create(shardID)
		if err == nil {
			filesMutex.Lock()
			files[shardID] = file
			filesMutex.Unlock()
		}

		return file, err
	}

	shards := make([][]byte, length)
	for j := uint64(0); j < length; j++ {
		shards[j] = make([]byte, info.ShardSize)
	}

	minSuccessWriters := length

	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()

	// FIXME: add checksum checks here

	if shardChecksums, checksum, err = Write(getShardWriter, shards, info, randReader(), minSuccessWriters); err != nil {
		t.Fatal(err)
	}

	return shardChecksums, checksum
}

func TestWrite(t *testing.T) {
	testCases := []struct {
		info           *Info
		shardChecksums []string
		checksum       string
	}{
		{
			info: &Info{
				DataCount:   1,
				ParityCount: 3,
				Size:        32283,
				ShardSize:   MiB,
			},
			shardChecksums: []string{
				"53e488c20a4168a2d093f7d221e649582f87ccb54124bf85afa4fb5619211621",
				"53e488c20a4168a2d093f7d221e649582f87ccb54124bf85afa4fb5619211621",
				"53e488c20a4168a2d093f7d221e649582f87ccb54124bf85afa4fb5619211621",
				"53e488c20a4168a2d093f7d221e649582f87ccb54124bf85afa4fb5619211621",
			},
			checksum: "53e488c20a4168a2d093f7d221e649582f87ccb54124bf85afa4fb5619211621",
		},
		{
			info: &Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283,
				ShardSize:   MiB,
			},
			shardChecksums: []string{
				"f8fb24aca71be9186d77b37b81190afcd22f11e36e5f16f71f8e07c17d13d6b6",
				"a196fd2517f9f945d857c62f7f89057f39cb46c357f94db8d90c31215ea6e640",
				"44c1cedc39cbef10004609f8a8ab2e3aa2b9ca1610a2a915c841fd6131e9c3e0",
				"10541be884895b77906d47da5cadd5f5f0bf84b8eb2a50219d037ea9de7780dd",
				"532de7275eb1f7c3a366ae494440c247e4a060936e47e23e430f49f95f4a0ba1",
				"1268977c0a3eb6693791c915ffc67f54e48f77ab350ac989d4ca2c490d2a9e3e",
				"804340a32a8894e74ae22292242504df2077b7d7229da7f31ce6b09f0d3f3a02",
				"f95052aa69bf8b2fd154c64660a002aa0041edc78db75f9c4f3f155d7dbdc6bd",
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &Info{
				DataCount:   4,
				ParityCount: 2,
				Size:        32283,
				ShardSize:   MiB,
			},
			shardChecksums: []string{
				"f8fb24aca71be9186d77b37b81190afcd22f11e36e5f16f71f8e07c17d13d6b6",
				"a196fd2517f9f945d857c62f7f89057f39cb46c357f94db8d90c31215ea6e640",
				"44c1cedc39cbef10004609f8a8ab2e3aa2b9ca1610a2a915c841fd6131e9c3e0",
				"10541be884895b77906d47da5cadd5f5f0bf84b8eb2a50219d037ea9de7780dd",
				"532de7275eb1f7c3a366ae494440c247e4a060936e47e23e430f49f95f4a0ba1",
				"1268977c0a3eb6693791c915ffc67f54e48f77ab350ac989d4ca2c490d2a9e3e",
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &Info{
				DataCount:   4,
				ParityCount: 7,
				Size:        32283,
				ShardSize:   MiB,
			},
			shardChecksums: []string{
				"f8fb24aca71be9186d77b37b81190afcd22f11e36e5f16f71f8e07c17d13d6b6",
				"a196fd2517f9f945d857c62f7f89057f39cb46c357f94db8d90c31215ea6e640",
				"44c1cedc39cbef10004609f8a8ab2e3aa2b9ca1610a2a915c841fd6131e9c3e0",
				"10541be884895b77906d47da5cadd5f5f0bf84b8eb2a50219d037ea9de7780dd",
				"532de7275eb1f7c3a366ae494440c247e4a060936e47e23e430f49f95f4a0ba1",
				"1268977c0a3eb6693791c915ffc67f54e48f77ab350ac989d4ca2c490d2a9e3e",
				"804340a32a8894e74ae22292242504df2077b7d7229da7f31ce6b09f0d3f3a02",
				"f95052aa69bf8b2fd154c64660a002aa0041edc78db75f9c4f3f155d7dbdc6bd",
				"264088d2891f10eaf5c17951d9dba7d3d6186213f99002ee3a867cb6ac107184",
				"9a45db8269b69b711428b8c8c8aef3eddfb71011e054182e004861e22c1c6bf4",
				"9e71bb6a506316d7ab636ade1d88e9e47cb6bbf074b784719b277f2144c3470d",
			},
			checksum: "9e544b64664932a07875af7d73a377d5f4bf150f2a2ab1a1138aa91f1f71066c",
		},
		{
			info: &Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        MiB,
				ShardSize:   MiB,
			},
			shardChecksums: []string{
				"e11379c54a0544a9e75731cd7d74b0597d43f448c29982fff9728f6bca09c4f7",
				"0790cb6b32ab6a9efe6af283cc53c9098d0e6fd3f3d4672b406e4b638f95edf1",
				"059d01add24e9b967bf8450a4bad25c1ae1fd1a157e1ac017bfb6a234edc5ce9",
				"f14989c3195059357d8b8da65f4aab328e96089bd0ac0d36b71951843652d8be",
				"7dc31349a96ef557eccac05245b2bf86ff2106f99296d8f8a023874388ab79aa",
				"e95df34f2b16c5e9f50c3635a29e02582ddef49c5928cbe63e0deab71e12eb96",
				"d145c1e856400de39d899e67c42c775d91feb535acf18845b3b3ef288f389329",
				"6cd8c4ab23ef33b0419aa27375b6cb9b54f21571969fce63d2b53e778bb700ee",
			},
			checksum: "85e459b0a2124d84a5caf33c7db50bd357ed6779fb211684c5bded824f99e7b5",
		},
		{
			info: &Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        4 * MiB,
				ShardSize:   MiB,
			},
			shardChecksums: []string{
				"85e459b0a2124d84a5caf33c7db50bd357ed6779fb211684c5bded824f99e7b5",
				"0073db12daafd05ff11c8d03d9d136fc240a7348a011ec3fd814f3965b35e192",
				"a488187bf215bc92c6f25986b375dde92e2717e39ebc13d2d93e0e52588230f4",
				"130800bf9c379a12bf348dd4d4659e3b660c6be08a73fd5714ad12f956139174",
				"0224d0d83c2db8c8fb7da29b6591d71c1e537c6dbcdeb371c258b49f92eaf47c",
				"817bb646414918daa4046d647edd23ea9c5370485108247b48da83b8ba281194",
				"d67a3b45aab6baa30a1a19ebafff48a586551292c45f155b6b5ce2628ee780bf",
				"d0db9b11e75315565233cf980b6c9aec2209dc563c874cf4e156e4ab0ae66f94",
			},
			checksum: "782a82eeeec6117e8cf251539b6152859732943867949f0207ff7eec8a7e7278",
		},
		{
			info: &Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        2 * 4 * MiB,
				ShardSize:   MiB,
			},
			shardChecksums: []string{
				"7faefe88ddb818f2cef02a0182f80bc29307e550f30fc9650e1a2f8ac6bc6f8e",
				"9d8aaeeebe67a5a6cfa5f8fa9c8758eb3d59e2351994d56c0e1696a909246c17",
				"0c329bacfc10836dff6d56db9e88d5196833db42a24b4681d4a03eb2968a014c",
				"c57ab4f5ecda838facd6c46a501bcb8c9121814a15cc2cbac815581109760f25",
				"14ed187872e040548633df9785bdbb8d16c9e2cec7a5aa0d8aa2ba28a926e5c1",
				"27e665acd1817414711ae96c08fd98e2b9b045252284e95092841f120f7f5ab9",
				"bd44de2d4b06366849623630d5b4b336179852ab87c83b39775b1f66119876e4",
				"456c6bd387508f26c80690006fbfe7e7f007190e9551bb9f7213d9699bace506",
			},
			checksum: "e9e4dc0e71b4c49ea20398c527de26854e00e9fda90a666b1cad2baef08d4c21",
		},
		{
			info: &Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283 + MiB,
				ShardSize:   MiB,
			},
			shardChecksums: []string{
				"998a1bcd0c5d0ddbfcc5c8a81119c2a42d73dcee2ce5e0ef24a04a8df4e530b0",
				"ddb6234ced4b3cb1811a4fca79ae9b1337fed233b0ca4aaf70c95e016c484bbf",
				"a29def081c34ee71891af4496746ff71388cd080b329987c998f865506b065ed",
				"a5293700bdf09e93541030157985dd3050d100733e799c10c99449164b34203c",
				"3d2e9e38a7f7c6246893325c3204361691917f1528845f9b912b5874c2c87b71",
				"0849f4dd855a4851e34a5b15360c80cf30bbcaccb4cbcd2bf8c5bba8b5e2c515",
				"e8fe445dd0d01e85bec7c15eeb8d8ae0be497a5728126aa21c07bc280b637eed",
				"8eeb86866cadb39294275ae2f6a8af62f9916f48fe26f1e6ef15592b603294bb",
			},
			checksum: "4ae7f09322921debd873f53bd726b0c0e6d04d96e8496166cb015d3ed12965df",
		},
		{
			info: &Info{
				DataCount:   4,
				ParityCount: 4,
				Size:        32283 + 4*MiB,
				ShardSize:   MiB,
			},
			shardChecksums: []string{
				"28fa77f0435d43b3efeebfdb06a506814b8b993ae764eac8cf6d9c57b6264a8b",
				"0c9caf443be6abf6fffeb43e59ce13dcae7af6d9b0f7ccd194075d71a645f1a4",
				"2bebde828c3d88b3a842dee2d16d270f30773420f7e337717c454ab9b0b73060",
				"b4a72cfb5f95fe7cf477ea9f3c30ea3516dcd2343bb0e9fb2fc057b7775e63f3",
				"11a3defb469e5048d6cf248b5e6ee46c78975667852d997bd5d4a212ba073519",
				"c1fd66806726e5351b129f9d66f6a629bfb6314470625be6386dc5f1f05962ea",
				"1e1da093fb3ae010f02aeabe62c2914c9c04aac248e238001ba7c69f969ae8cb",
				"66983ed9b6ce410cc5929c51f4b6e76498adc773ac06f4212ce4c5abf2a38e7b",
			},
			checksum: "bd77d467172842b58066fffbf126bd6fa8c8e04ff3607c4500ec6f35c53f1fc1",
		},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v", i),
			func(t *testing.T) {
				dirname := xrand.NewID(8).String()
				defer os.RemoveAll(dirname)
				shardChecksums, checksum := testWrite(t, testCase.info, dirname)

				if !reflect.DeepEqual(shardChecksums, testCase.shardChecksums) {
					t.Fatalf("expected: shardChecksums: %v, got: %v", testCase.shardChecksums, shardChecksums)
				}

				if checksum != testCase.checksum {
					t.Fatalf("expected: checksum: %v, got: %v", testCase.checksum, checksum)
				}
			},
		)
	}
}
