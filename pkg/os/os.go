package os

import (
	"errors"
	"os"
	"path"
	"syscall"
)

// Available functions:
// func Exist(name string) bool

// CreatePath creates path tree; if temp file is given, it is moved as tail entry of path.
func CreatePath(cpath string, tempFile string, errorOnTailExist bool) error {
	mkdirAll := os.MkdirAll
	if errorOnTailExist {
		mkdirAll = func(name string, perm os.FileMode) error {
			if err := os.MkdirAll(path.Dir(name), perm); err != nil {
				return err
			}

			return os.Mkdir(name, perm)
		}
	}

	if tempFile == "" {
		return mkdirAll(cpath, 0755)
	}

	if err := os.MkdirAll(path.Dir(cpath), 0755); err != nil {
		return err
	}

	return os.Rename(tempFile, cpath)
}

// RemovePath removes path tree up to base directory when each sub-tree path is empty. It removes all children in path tree if removeChildren flag is set.
func RemovePath(rpath, base string, removeChildren bool) (err error) {
	if removeChildren {
		err = os.RemoveAll(rpath)
		switch {
		case err == nil, errors.Is(err, os.ErrNotExist):
		case errors.Is(err, syscall.ENOTEMPTY):
			return nil
		default:
			return err
		}

		rpath = path.Dir(rpath)
	}

	for rpath != base && rpath != "." && rpath != "/" {
		err = os.Remove(rpath)
		switch {
		case err == nil, errors.Is(err, os.ErrNotExist):
		case errors.Is(err, syscall.ENOTEMPTY):
			return nil
		default:
			return err
		}

		rpath = path.Dir(rpath)
	}

	return nil
}

// func CopyFile(srcFilename, destFilename string) (err error) {
// 	var srcFile *os.File
// 	var destFile *os.File
// 	destFileCreated := false
//
// 	defer func() {
// 		if srcFile != nil {
// 			srcFile.Close()
// 		}
//
// 		if destFile != nil {
// 			destFile.Close()
// 		}
//
// 		if err != nil && destFileCreated {
// 			os.Remove(destFilename)
// 		}
// 	}()
//
// 	if srcFile, err = os.Open(srcFilename); err != nil {
// 		return err
// 	}
//
// 	fi, err := srcFile.Stat()
// 	if err != nil {
// 		return err
// 	}
//
// 	if !fi.Mode().IsRegular() {
// 		return fmt.Errorf("source %v is not regular file", srcFilename)
// 	}
//
// 	if destFile, err = os.OpenFile(destFilename, os.O_WRONLY|os.O_CREATE, os.ModePerm); err != nil {
// 		return err
// 	}
// 	destFileCreated = true
//
// 	_, err = io.Copy(destFile, srcFile)
// 	return err
// }
//
// func MksubdirAll(base, name string) error {
// 	for _, subDir := range strings.Split(name, "/") {
// 		if subDir == "" {
// 			continue
// 		}
//
// 		base = path.Join(base, subDir)
// 		if err := os.Mkdir(base, os.ModePerm); err != nil && !os.IsExist(err) {
// 			return err
// 		}
// 	}
//
// 	return nil
// }
//
// func MkdirAllX(name string, perm os.FileMode) (parent string, err error) {
// 	parent = name
// 	for {
// 		if err = os.Mkdir(parent, perm); err == nil {
// 			break
// 		}
//
// 		if os.IsExist(err) {
// 			err = nil
// 			break
// 		}
//
// 		if !os.IsNotExist(err) {
// 			return "", err
// 		}
//
// 		parent = path.Dir(parent)
// 	}
//
// 	if parent != name {
// 		err = os.MkdirAll(name, perm)
// 	}
//
// 	return parent, err
// }
//
// func RemoveAllX(name, root string) (parent string, err error) {
// 	if err = os.Remove(name); err != nil {
// 		return name, err
// 	}
//
// 	for {
// 		if parent = path.Dir(name); parent == root {
// 			break
// 		}
//
// 		if err = os.Remove(parent); err != nil {
// 			break
// 		}
//
// 		parent = path.Dir(parent)
// 	}
//
// 	if !os.IsNotExist(err) {
// 		return parent, err
// 	}
//
// 	return "", nil
// }
