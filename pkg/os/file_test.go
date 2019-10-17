package os

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"testing"

	xhash "github.com/balamurugana/goat/pkg/hash"
	xrand "github.com/balamurugana/goat/pkg/rand"
)

func randReader() io.Reader {
	return rand.New(rand.NewSource(271828))
}

func TestWriteFile(t *testing.T) {
	testCases := []struct {
		size      uint64
		checksum  string
		header    *checksumHeader
		checksums []string
	}{
		{
			size:     16279,
			checksum: "cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b",
			header: &checksumHeader{
				HashName:   "HighwayHash256",
				HashKey:    "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
				HashLength: 64,
				BlockSize:  1048576,
				BlockCount: 1,
				DataLength: 16279,
			},
			checksums: []string{
				"cfdb0f1b0043595e8913f22af69eead850eb249dffb41f545495cbe6dee9240b",
			},
		},

		{
			size:     70009289,
			checksum: "0232a959153a87b0f59d8491b68eff93f1b62596c96cf2c3cfde7ec52457e64d",
			header: &checksumHeader{
				HashName:   "HighwayHash256",
				HashKey:    "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
				HashLength: 64,
				BlockSize:  1048576,
				BlockCount: 67,
				DataLength: 70009289,
			},
			checksums: []string{
				"85e459b0a2124d84a5caf33c7db50bd357ed6779fb211684c5bded824f99e7b5",
				"0073db12daafd05ff11c8d03d9d136fc240a7348a011ec3fd814f3965b35e192",
				"a488187bf215bc92c6f25986b375dde92e2717e39ebc13d2d93e0e52588230f4",
				"130800bf9c379a12bf348dd4d4659e3b660c6be08a73fd5714ad12f956139174",
				"b2695f2a90d09ebdd0ad6deb224940213cd0d44e814bf99632ccb786f4025cef",
				"9f7ec6ee08d794142dfb57c9b10aff1f8ca5c6491dd61cb5f54a59be45b180ac",
				"04ea0cb70f3da95e4ed5a6f10d1fed636d71168aa31c75e439888448afc53162",
				"18920581e245724bd3998e6f40a473e8b78eb8c91bb9485bb26d7b32b8073683",
				"f6eecd65529bf6ddc97edb53f0d4ef05d9a7377fb10672a92cf6bbd22dcf99b9",
				"89cbd64f04952fdcdd5504ad77cf1a66871b84022d495c5c78f9edb7212832f0",
				"2518201e5efe401a3ae4044b2edc863ab4f6b68665d49e9734615f72baecb440",
				"31ca3440661ad4aa995cff2b55a95b75e7b3817a83d8dbb124b194425e24c98f",
				"498b6ab582bb519c18423b46cc3a6ca8f709acdb7c4efbbd0b6c59afe7bf71d6",
				"667acd35705d4a1c20438b9fe89b6c58a15dfe7f4507c71f91fa8795ab97b847",
				"cf46ab184391dd4af016c8ded897fd04c2c1f5de5fd83d7a8a1dc79ad69d1178",
				"a561511ee54434cf4c06a0739d04d2fd57287d88cd2e4e89fb933d8291bb9803",
				"b0d58b658b3a107e6ee00732ae109ac94a3059a3f960bf225b0ca47cc3b9215d",
				"0e9457ec1f26d940ec236984c4514a5062b91d46c930d22bc775755be087b29c",
				"5e77c7bee53f0147e5a78e24f56d080e9aeeb2e44b1927db53b052acba595c7c",
				"92099056bd2abae5fcae6a45c7b1c2cce3c987cfb9ed3735fea563f70329f029",
				"d59d85adb5a2ffab7542872b2d304ea416e426288761af08372f7cab9b5e0283",
				"bd17d35fb5a68e5602be98d3abebe1f024a08ec2d992dada08eb5746d626cec7",
				"3348ec3cf5ffbe554de36aed26946160f1d2b8cb627d059b84a08bb8775e7555",
				"f124ed6b35cbfa0cb9d6ec4e1c7a876968b4529ad41c83eb29a7aa65c93c4421",
				"f4a00184ca8fb256ad2f3030ece0355d2e3b1cd47667eee5d2a99e59a816d6db",
				"738d56dd56a44d6b99f6bd120528e4fb53788b62a7aaad902dd7737069f57ce2",
				"e8f646565019c76895604a671fdf637d8587883ed61f01e118fa301f0fe50f32",
				"4b03cac685599c722d6a92003e4ca4aab3ced9d7da4fc57399b9abc470cd6b88",
				"91e1bd31bbd37647f5af58a8d8131e619a97f938640f408cc9fd99924eafa7cc",
				"32fb1326693ea956127213c64180ad55a41db4807ff2804b0a251ac101a54d87",
				"e2e772be68e6a2efaee34cd34fff38bda06774327e06355092b8c616e874c6c7",
				"c5aa248b794792dd5a32d3d59318d87bae88500d2ba8d91a2677758855b12636",
				"a47740c921071ada8e7bd1c337bb8bf718087b44fc9acb3b5316c89db8b07519",
				"d1a9a1f47c9fa0c7b8d9fb3dbf3752df575cdcf259f69b2f62e6cf734a0be914",
				"f81012770f2d519ac644c4ec095c2637e910a99bd150f4c657067019ce15a6f2",
				"70077bcd385d84ee4f70748b1181312cd98865468cdb34c83e8aefdbaeadc89d",
				"d215b5e08c68d8bd89c8ccf96abd8935ed3fb9c01d7360f148a5bd2b82f42568",
				"8abb0364ab9ea4291f28514a244f14be31800acee0ffeda43e3ae44b4bfb8360",
				"d72d6dbbe4796d1c1908c0723abf76e6a5fa37d23cfb3304a8dde07077e489db",
				"f505f104e319dab18add19b1fb952db90c004057b1ca6bfe92c701c0a004f289",
				"c77bad02d83bcd09684517e6f0d2e794cb291bfa1dbb2abd22a5bf056942e4d6",
				"89046b34cb678a59cc6c50ce9bbfaf71edbcd537117e682a99517054ac6d0254",
				"d81b92db756066201f9a5aa19731d34802b6a0869a7b891dc66b98cb3983f23f",
				"bc770b6500c31b2c8910a3bb4ba0fcaacd67a5550c2964ae318f4acac6be704a",
				"a0d11aec0bf3d0940d5ff5195fa33005e2a95be41c14774c6f8c6651a1ced91c",
				"c1c64012f2fcd7bc7473a3b2309afc77115d087272828d5d172abc6f248e980b",
				"9282c64fec0b093827ffae2af07c6568130d12d70a146ec8a60019bc681237b8",
				"fa31e41e8a41b9eb1cb9b6d6f5755fce9040dfad2c026028e5f3b7c097cad3c0",
				"b52a09cf114d8e975011429ad1bc8fb3bea0d428d828f95d32486f1f87ef2597",
				"13ea43e0681c8b2a1a468f058ab5e720509888dc8bb885340f31acba3563a763",
				"520a60c9e4f759c7a15902eaa53bbfceb122b809a8e9735c4aba48b19652f105",
				"2d1633c1cb498e024252980667a1f1c768ca6096205eb36f5edf0e67d56585d9",
				"6f2bba844c3814858708e24cbe3a25744bcc2d346301500c6d39386fcbb2b8b3",
				"ec42b0d6914822b72606d93f0306af41264258ba695d705a72318beb3a0d3511",
				"aaeba3b35663d6033b632389fb6f4d5dee9201c27cf539aa2662a4303b39f2d9",
				"baa1c05b961cce3a1ac12e620c94be9ddeb6f65c17a323a6265c72835ebc9c3f",
				"bfe352dce31db3c90be1a009f84c32514809fce5b46262365397bdfab9308670",
				"7649851ba694a61f0fa47600ad7c723c62fdb1f1f03ce00033b75f4f102097bd",
				"9f6e60b76bf3289a6f770e3beecbcd6a5dd58bfd0a7a3e935da4ddf0e75ace99",
				"d2c9dc0283199534b5e8e18157f3e0713003f516feb4939d37f7178b79ca17f2",
				"a7fac74a80681e405f1289d9ec17cadda4f67a3422dd2c0e1eca6c3e41f2abdf",
				"2e87222a4333952c88d4977457f837a875f93e191c2afd894e61e65a5ccf00cd",
				"7fbcab130260975a64dcbc8c3e87d6c55078651948b3e57795583d50f3fbcdd2",
				"a5cfec86a808b51596b0361d7efa550ab6a23b75667c4e1c78289952db55056c",
				"52a98cd94d0808ae4d0defb14a4a2b6cb53b509dd2dc02950fbbbece6dfbdc26",
				"f65a5ac20aa04a8a10a63d44c213fda4ad698766c02de5c491e67d31ecf75618",
				"494f8fa7e1c9aa832353d0e3e07e407bcf142d02e570f5f73a6388cb6220a09f",
			},
		},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v-without-bitrot", i),
			func(t *testing.T) {
				filename := xrand.NewID(8).String()

				checksum, err := WriteFile(filename, randReader(), testCase.size, false)
				if err != nil {
					t.Fatal(err)
				}

				defer func() {
					os.Remove(filename)
				}()

				if checksum != testCase.checksum {
					t.Fatalf("checksum mismatch. expected: %v, got: %v", testCase.checksum, checksum)
				}
			})

		t.Run(
			fmt.Sprintf("test%v-with-bitrot", i),
			func(t *testing.T) {
				filename := xrand.NewID(8).String()
				checksum, err := WriteFile(filename, randReader(), testCase.size, true)
				if err != nil {
					t.Fatal(err)
				}

				defer func() {
					os.Remove(filename)
					os.Remove(filename + ".checksum")
				}()

				if checksum != testCase.checksum {
					t.Fatalf("checksum mismatch. expected: %v, got: %v", testCase.checksum, checksum)
				}

				csfile, err := openChecksumFile(filename)
				if err != nil {
					t.Fatalf("failed to open file %v: %v", filename, err)
				}

				if !reflect.DeepEqual(testCase.header, csfile.header) {
					t.Fatalf("header mismatch. expected: %+v, got: %+v", testCase.header, csfile.header)
				}

				for j, expected := range testCase.checksums {
					got, err := csfile.ReadSum()
					if err != nil {
						t.Fatalf("block %v: error %v", j+1, err)
					}

					if expected != got {
						t.Fatalf("block %v; checksum mismatch. expected: %+v, got: %+v", j+1, expected, got)
					}
				}
			},
		)
	}
}

func TestRemoveFile(t *testing.T) {
	t.Run(
		fmt.Sprintf("without-bitrot"),
		func(t *testing.T) {
			filename := xrand.NewID(8).String()

			if _, err := WriteFile(filename, randReader(), 0, false); err != nil {
				t.Fatal(err)
			}

			if err := RemoveFile(filename, false); err != nil {
				t.Fatal(err)
			}
		},
	)

	t.Run(
		fmt.Sprintf("with-bitrot"),
		func(t *testing.T) {
			filename := xrand.NewID(8).String()

			if _, err := WriteFile(filename, randReader(), 0, true); err != nil {
				t.Fatal(err)
			}

			if err := RemoveFile(filename, true); err != nil {
				t.Fatal(err)
			}
		},
	)
}

func TestOpenFile(t *testing.T) {
	testCases := []struct {
		size   uint64
		bitrot bool
		offset int64
		length uint64
		hash   string
	}{
		{16279, false, 0, 10, "cb681256c303aaacfc24ed94cb5ffd6a84fcde8a6721213b0a757ba40ac4a4a9"},
		{16279, true, 0, 10, "cb681256c303aaacfc24ed94cb5ffd6a84fcde8a6721213b0a757ba40ac4a4a9"},
		{16279, false, 10, 7, "aa88fcc3f216be54199c57fd835b9921a6fd259edc834d115b6b898ccfaa4c25"},
		{16279, true, 10, 7, "aa88fcc3f216be54199c57fd835b9921a6fd259edc834d115b6b898ccfaa4c25"},
		{70009289, false, 3145649, 1048986, "3faf5850c140d6f2ad36e0ba7324d306e1589d50fc17fa0cc1a1ccbf76d87332"},
		{70009289, true, 3145649, 1048986, "3faf5850c140d6f2ad36e0ba7324d306e1589d50fc17fa0cc1a1ccbf76d87332"},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("test%v-bitrot=%v", i, testCase.bitrot),
			func(t *testing.T) {
				filename := xrand.NewID(8).String()

				if _, err := WriteFile(filename, randReader(), testCase.size, testCase.bitrot); err != nil {
					t.Fatal(err)
				}

				defer func() {
					if err := RemoveFile(filename, testCase.bitrot); err != nil {
						t.Error(err)
					}
				}()

				rc, err := OpenFile(filename, testCase.offset, testCase.length, testCase.bitrot)
				if err != nil {
					t.Fatal(err)
				}

				defer func() {
					if err := rc.Close(); err != nil {
						t.Error(err)
					}
				}()

				hasher := xhash.MustGetNewHash(xhash.HighwayHash256Algorithm, nil)
				if _, err = io.Copy(hasher, rc); err != nil {
					t.Fatal(err)
				}

				checksum := hasher.HexSum(nil)

				if checksum != testCase.hash {
					t.Fatalf("expected: %v, got: %v", testCase.hash, checksum)
				}
			},
		)
	}
}
