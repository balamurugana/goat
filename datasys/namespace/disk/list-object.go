package disk

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	xos "github.com/balamurugana/goat/pkg/os"
)

const (
	objectID      = "yjf-iAepYVVTZIyO6tXZ1Ghz6iAZOwmVtGqg6y_or2s.default" // Base64 encoded Highway256 hash of string 'object' + ".default"
	slashObjectID = "Q5C0uOv_8zmsnKK27G_pCfdaDWyJnaWyJFbwKUNEMzM.default" // Base64 encoded Highway256 hash of string '/' + ".default"
)

func listRecursive(rootDir, dirName, prefix, startAfter string, objectCheck, slashObjectCheck bool, maxKeys *int, objects *[]string, more *bool) (err error) {
	if *maxKeys <= 0 {
		return nil
	}

	subdirNames := []string{}
	objectFound := false
	slashObjectFound := false

	picker := func(name string, mode os.FileMode) (stop bool) {
		switch {
		case mode.IsDir():
		case mode.IsRegular():
			if objectCheck || slashObjectCheck {
				switch name {
				case objectID:
					objectFound = objectCheck
				case slashObjectID:
					slashObjectFound = slashObjectCheck
				}
			}
			return false
		default:
			return false
		}

		name = path.Join(dirName, name)

		if !strings.HasPrefix(name, prefix) {
			return false
		}

		if strings.Compare(name, startAfter) < 0 {
			return false
		}

		subdirNames = append(subdirNames, name)
		return false
	}

	if err = xos.Readdirnames(path.Join(rootDir, dirName), picker); err != nil {
		return err
	}

	name := path.Join(dirName, "")
	if objectFound {
		if strings.HasPrefix(name, prefix) && strings.Compare(name, startAfter) > 0 {
			subdirNames = append(subdirNames, name)
		}
	}

	if slashObjectFound {
		name += "/"
		if strings.HasPrefix(name, prefix) && strings.Compare(name, startAfter) > 0 {
			subdirNames = append(subdirNames, name)
		}
	}

	sort.Strings(subdirNames)

	if !*more {
		*more = len(subdirNames) > *maxKeys
	}
	if len(subdirNames) > *maxKeys {
		subdirNames = subdirNames[:*maxKeys]
	}

	for i, subdirName := range subdirNames {
		switch subdirName {
		case dirName, dirName + "/":
			*objects = append(*objects, subdirName)

			if *maxKeys--; *maxKeys == 0 {
				if len(subdirNames) > i+1 {
					*more = true
				}
			}
		default:
			if err = listRecursive(rootDir, subdirName, prefix, startAfter, true, true, maxKeys, objects, more); err != nil {
				return err
			}
		}

		if *maxKeys == 0 {
			if len(subdirNames) > i+1 {
				*more = true
			}

			break
		}
	}

	return nil
}

func listDirRecursive(rootDir, prefix, startAfter string, maxKeys int) (objects []string, isTruncated bool, nextMarker string, err error) {
	if strings.HasPrefix(prefix, "/") {
		return objects, isTruncated, nextMarker, fmt.Errorf("prefix must not start with '/'")
	}

	if strings.HasPrefix(startAfter, "/") {
		return objects, isTruncated, nextMarker, fmt.Errorf("startAfter must not start with '/'")
	}

	dirName := ""
	objectCheck := false
	slashObjectCheck := true

	if prefix != "" {
		dirName = prefix
		if !strings.HasSuffix(prefix, "/") {
			dirName, _ = path.Split(prefix)
			objectCheck = true
		}
	}

	if startAfter != "" && strings.HasPrefix(startAfter, prefix) {
		dirName = startAfter
		objectCheck = false
		if strings.HasSuffix(startAfter, "/") {
			slashObjectCheck = false
		} else {
			dirName, _ = path.Split(startAfter)
			slashObjectCheck = true
		}

		if strings.HasSuffix(dirName, "/") {
			dirName = strings.TrimSuffix(dirName, "/")
		}

		base := ""
		for {
			err = listRecursive(rootDir, dirName, prefix, startAfter, objectCheck, slashObjectCheck, &maxKeys, &objects, &isTruncated)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return objects, isTruncated, nextMarker, err
				}
			}

			if base = path.Base(dirName); base == "." {
				base = ""
			}
			if dirName = path.Dir(dirName); dirName == "." {
				dirName = ""
			}
			if base == "" && (dirName == "" || dirName == "/") {
				break
			}
		}
	} else {
		err = listRecursive(rootDir, dirName, prefix, startAfter, objectCheck, slashObjectCheck, &maxKeys, &objects, &isTruncated)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return objects, isTruncated, nextMarker, err
			}
		}
	}

	if isTruncated && len(objects) > 0 {
		nextMarker = objects[len(objects)-1]
	}

	return objects, isTruncated, nextMarker, nil
}

func isDir(dir string) bool {
	found := false
	xos.Readdirnames(dir, func(name string, mode os.FileMode) (stop bool) {
		found = mode.IsDir()
		return found
	})

	return found
}

func list(rootDir, dirName, startsWith, startAfter string, maxKeys int) (objects []string, prefixes []string, more bool, err error) {
	if maxKeys <= 0 {
		return nil, nil, false, nil
	}

	subdirNames := []string{}

	slashObjectCheck := false
	if strings.HasSuffix(dirName, "/") {
		slashObjectCheck = true
		dirName = strings.TrimSuffix(dirName, "/")
	}

	picker := func(name string, mode os.FileMode) (stop bool) {
		switch {
		case mode.IsRegular():
			if !slashObjectCheck || name != slashObjectID {
				return false
			}
			name = "/"
		case mode.IsDir():
		default:
			return false
		}

		if !strings.HasPrefix(name, startsWith) {
			return false
		}

		if name == "/" {
			name = path.Join(dirName, name) + "/"
		} else {
			name = path.Join(dirName, name)
		}

		switch strings.Compare(name, startAfter) {
		case 0:
			if strings.HasSuffix(startAfter, "/") {
				return false
			}

			subdir := path.Join(rootDir, name)
			if !xos.Exist(path.Join(subdir, slashObjectID)) && !isDir(subdir) {
				return false
			}

			if !strings.HasSuffix(name, "/") {
				name += "/"
			}
		case 1:
		default:
			return false
		}

		subdirNames = append(subdirNames, name)
		return false
	}

	if err = xos.Readdirnames(path.Join(rootDir, dirName), picker); err != nil {
		return nil, nil, false, err
	}

	sort.Strings(subdirNames)

	more = len(subdirNames) > maxKeys
	if len(subdirNames) > maxKeys {
		subdirNames = subdirNames[:maxKeys]
	}

	for i, subdirName := range subdirNames {
		switch subdirName {
		case dirName + "/":
			objects = append(objects, subdirName)

			if maxKeys--; maxKeys == 0 {
				if len(subdirNames) > i+1 {
					more = true
				}
			}
		default:
			subdir := path.Join(rootDir, subdirName)

			isPrefix := strings.HasSuffix(subdirName, "/") || xos.Exist(path.Join(subdir, slashObjectID)) || isDir(subdir)

			if !strings.HasSuffix(subdirName, "/") && xos.Exist(path.Join(subdir, objectID)) {
				objects = append(objects, subdirName)

				if maxKeys--; maxKeys == 0 {
					if len(subdirNames) > i+1 || isPrefix {
						more = true
					}
				}
			}

			if isPrefix && maxKeys > 0 {
				if !strings.HasSuffix(subdirName, "/") {
					subdirName += "/"
				}
				prefixes = append(prefixes, subdirName)

				if maxKeys--; maxKeys == 0 {
					if len(subdirNames) > i+1 {
						more = true
					}
				}
			}
		}

		if maxKeys == 0 {
			break
		}
	}

	return objects, prefixes, more, nil
}

func listDir(rootDir, prefix, startAfter string, maxKeys int) (objects []string, prefixes []string, more bool, nextMarker string, err error) {
	if strings.HasPrefix(prefix, "/") {
		return objects, prefixes, more, nextMarker, fmt.Errorf("prefix must not start with '/'")
	}

	if strings.HasPrefix(startAfter, "/") {
		return objects, prefixes, more, nextMarker, fmt.Errorf("startAfter must not start with '/'")
	}

	dirName := ""
	startsWith := ""

	if prefix != "" {
		dirName = prefix

		if !strings.HasSuffix(prefix, "/") {
			dirName, startsWith = path.Split(prefix)
		}
	}

	objects, prefixes, more, err = list(rootDir, dirName, startsWith, startAfter, maxKeys)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return objects, prefixes, more, nextMarker, err
		}
	}

	if more {
		if len(objects) > 0 {
			nextMarker = objects[len(objects)-1]
		}

		if len(prefixes) > 0 && strings.Compare(prefixes[len(prefixes)-1], nextMarker) > 0 {
			nextMarker = prefixes[len(prefixes)-1]
		}
	}

	return objects, prefixes, more, nextMarker, nil
}

func ListObjects(rootDir, prefix, startAfter string, maxKeys int, isRecursive bool) (objects, prefixes []string, isTruncated bool, nextMarker string, err error) {
	if isRecursive {
		objects, isTruncated, nextMarker, err = listDirRecursive(rootDir, prefix, startAfter, maxKeys)
	} else {
		objects, prefixes, isTruncated, nextMarker, err = listDir(rootDir, prefix, startAfter, maxKeys)
	}

	return
}
