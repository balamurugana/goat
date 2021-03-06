package rpc

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func gobEncode(e interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(e)
	return buf.Bytes(), err
}

type Args struct {
	A, B int
}

func (a *Args) Authenticate() (err error) {
	if a.A == 0 && a.B == 0 {
		err = errors.New("authenticated failed")
	}

	return
}

type Quotient struct {
	Quo, Rem int
}

type Arith struct{}

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

func (t *Arith) MultiplyInputStream(args *Args, reader io.Reader, reply *int) (io.ReadCloser, error) {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	if string(data) != "multiply reader request" {
		return nil, fmt.Errorf("expected: message: multiply reader request, got: %s", string(data))
	}

	*reply = args.A * args.B

	return nil, nil
}

func (t *Arith) MultiplyOutputStream(args *Args, reader io.Reader, reply *int) (io.ReadCloser, error) {
	*reply = args.A * args.B

	var buf bytes.Buffer
	data := []byte("reply")
	for i := 0; i < *reply; i++ {
		if n, err := buf.Write(data); err != nil {
			return nil, err
		} else if n != len(data) {
			return nil, errors.New("short write")
		}
	}

	return ioutil.NopCloser(&buf), nil
}

func (t *Arith) MultiplyIOStream(args *Args, reader io.Reader, reply *int) (io.ReadCloser, error) {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	*reply = args.A * args.B

	var buf bytes.Buffer
	for i := 0; i < *reply; i++ {
		if n, err := buf.Write(data); err != nil {
			return nil, err
		} else if n != len(data) {
			return nil, errors.New("short write")
		}
	}

	return ioutil.NopCloser(&buf), nil
}

type mytype int

type Auth struct{}

func (a Auth) Authenticate() error {
	return nil
}

// exported method.
func (t mytype) Foo(a *Auth, b *int) error {
	return nil
}

// incompatible method because of unexported method.
func (t mytype) foo(a *Auth, b *int) error {
	return nil
}

// incompatible method because of first argument is not Authenticator.
func (t *mytype) Bar(a, b *int) error {
	return nil
}

// incompatible method because of error is not returned.
func (t mytype) IncompatFoo(a, b *int) {
}

// incompatible method because of second argument is not a pointer.
func (t *mytype) IncompatBar(a *int, b int) error {
	return nil
}

func TestIsExportedOrBuiltinType(t *testing.T) {
	var i int
	case1Type := reflect.TypeOf(i)

	var iptr *int
	case2Type := reflect.TypeOf(iptr)

	var a Arith
	case3Type := reflect.TypeOf(a)

	var aptr *Arith
	case4Type := reflect.TypeOf(aptr)

	var m mytype
	case5Type := reflect.TypeOf(m)

	var mptr *mytype
	case6Type := reflect.TypeOf(mptr)

	testCases := []struct {
		t              reflect.Type
		expectedResult bool
	}{
		{case1Type, true},
		{case2Type, true},
		{case3Type, true},
		{case4Type, true},
		// Type.Name() starts with lower case and Type.PkgPath() is not empty.
		{case5Type, false},
		// Type.Name() starts with lower case and Type.PkgPath() is not empty.
		{case6Type, false},
	}

	for i, testCase := range testCases {
		result := isExportedOrBuiltinType(testCase.t)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestGetMethodMap(t *testing.T) {
	var a Arith
	case1Type := reflect.TypeOf(a)

	var aptr *Arith
	case2Type := reflect.TypeOf(aptr)

	var m mytype
	case3Type := reflect.TypeOf(m)

	var mptr *mytype
	case4Type := reflect.TypeOf(mptr)

	testCases := []struct {
		t              reflect.Type
		expectedResult int
	}{
		// No methods exported.
		{case1Type, 0},
		// Multiply, Divide, MultiplyInputStream, MultiplyOutputStream and MultiplyIOStream methods are exported.
		{case2Type, 5},
		// Foo method is exported.
		{case3Type, 1},
		// Foo method is exported.
		{case4Type, 1},
	}

	for i, testCase := range testCases {
		m := getMethodMap(testCase.t)
		result := len(m)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestServerRegisterName(t *testing.T) {
	case1Receiver := &Arith{}
	var case2Receiver mytype
	var case3Receiver *Arith
	i := 0
	var case4Receiver = &i
	var case5Receiver Arith

	testCases := []struct {
		name      string
		receiver  interface{}
		expectErr bool
	}{
		{"Arith", case1Receiver, false},
		{"arith", case1Receiver, false},
		{"Arith", case2Receiver, false},
		// nil receiver error.
		{"Arith", nil, true},
		// nil receiver error.
		{"Arith", case3Receiver, true},
		// rpc.Register: type Arith has no exported methods of suitable type error.
		{"Arith", case4Receiver, true},
		// rpc.Register: type Arith has no exported methods of suitable type (hint: pass a pointer to value of that type) error.
		{"Arith", case5Receiver, true},
	}

	for i, testCase := range testCases {
		err := NewServer().RegisterName(testCase.name, testCase.receiver)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestServerCall(t *testing.T) {
	server1 := NewServer()
	if err := server1.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	server2 := NewServer()
	if err := server2.RegisterName("arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	case1ArgBytes, err := gobEncode(&Args{7, 8})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	reply := 7 * 8
	case1ExpectedResult, err := gobEncode(&reply)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	case2ArgBytes, err := gobEncode(&Args{})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		server         *Server
		serviceMethod  string
		argBytes       []byte
		expectedResult []byte
		expectErr      bool
	}{
		{server1, "Arith.Multiply", case1ArgBytes, case1ExpectedResult, false},
		{server2, "arith.Multiply", case1ArgBytes, case1ExpectedResult, false},
		// invalid service/method request ill-formed error.
		{server1, "Multiply", nil, nil, true},
		// can't find service error.
		{server1, "arith.Multiply", nil, nil, true},
		// can't find method error.
		{server1, "Arith.Add", nil, nil, true},
		// gob decode error.
		{server1, "Arith.Multiply", []byte{10}, nil, true},
		// authentication error.
		{server1, "Arith.Multiply", case2ArgBytes, nil, true},
	}

	for i, testCase := range testCases {
		var replyBuf bytes.Buffer
		_, err := testCase.server.call(testCase.serviceMethod, testCase.argBytes, nil, &replyBuf)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			result := replyBuf.Bytes()
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestServerServeHTTP(t *testing.T) {
	server1 := NewServer()
	if err := server1.RegisterName("Arith", &Arith{}); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	argBytes, err := gobEncode(&Args{7, 8})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	requestBodyData, err := gobEncode(callRequest{Method: "Arith.Multiply", ArgBytes: argBytes})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case1Request, err := http.NewRequest("POST", "http://localhost:12345/", bytes.NewReader(requestBodyData))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	reply := 7 * 8
	replyBytes, err := gobEncode(&reply)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case1Result, err := gobEncode(callResponse{ReplyBytes: replyBytes})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	case2Request, err := http.NewRequest("GET", "http://localhost:12345/", bytes.NewReader([]byte{}))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	case3Request, err := http.NewRequest("POST", "http://localhost:12345/", bytes.NewReader([]byte{10, 20}))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	requestBodyData, err = gobEncode(callRequest{Method: "Arith.Add", ArgBytes: argBytes})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case4Request, err := http.NewRequest("POST", "http://localhost:12345/", bytes.NewReader(requestBodyData))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	case4Result, err := gobEncode(callResponse{Error: "can't find method Add"})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		server         *Server
		httpRequest    *http.Request
		expectedCode   int
		expectedResult []byte
	}{
		{server1, case1Request, http.StatusOK, case1Result},
		{server1, case2Request, http.StatusMethodNotAllowed, nil},
		{server1, case3Request, http.StatusBadRequest, nil},
		{server1, case4Request, http.StatusOK, case4Result},
	}

	for i, testCase := range testCases {
		writer := httptest.NewRecorder()
		testCase.server.ServeHTTP(writer, testCase.httpRequest)
		if writer.Code != testCase.expectedCode {
			t.Fatalf("case %v: code: expected: %v, got: %v\n", i+1, testCase.expectedCode, writer.Code)
		}

		if testCase.expectedCode == http.StatusOK {
			result := writer.Body.Bytes()
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}
