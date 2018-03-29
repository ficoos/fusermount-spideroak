package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

var shareID = flag.String("share-id", "", "name of the share")
var roomKey = flag.String("room-key", "", "share's room key")
var cacheSize uint64 = 0
var cacheDir = flag.String("cache-dir", ".cache", "cache directory")
var help bool = false

func init() {
	flag.Var(&SizeVar{&cacheSize}, "cache-size", "size of the cache (default 1G)")
	flag.BoolVar(&help, "h", false, "display this help and exit")
	flag.BoolVar(&help, "help", false, "display this help and exit")
}

type SizeVar struct {
	V *uint64
}

func (sv *SizeVar) String() string {
	return "size"
}

func (sv *SizeVar) Set(s string) error {
	if len(s) == 0 {
		*sv.V = 0
		return nil
	}
	var mult uint64 = 1
	multChar := s[len(s)-1]
	if multChar < '0' || multChar > '9' {
		s = s[:len(s)-1]
		switch multChar {
		case 'b', 'B':
			mult = 1
		case 'k', 'K':
			mult = 1 << 10
		case 'm', 'M':
			mult = 1 << 20
		case 'g', 'G':
			mult = 1 << 30
		case 't', 'T':
			mult = 1 << 40
		default:
			return errors.New("Invalid size specifier valid entries are (b, k, m, g, t)")
		}
	}

	if res, err := strconv.ParseUint(s, 10, 8); err != nil {
		return err
	} else {
		*sv.V = res * mult
		return nil
	}
}

const FILE_MODE = fuse.S_IFREG | 0444
const DIR_MODE = fuse.S_IFDIR | 0555

type ShareFile struct {
	ino           *nodefs.Inode
	fileEntry     FileEntry
	backingReader ReadAtCloser
}

func NewShareFile(fileEntry FileEntry, backingReader ReadAtCloser) nodefs.File {
	return &ShareFile{
		ino:           nil,
		fileEntry:     fileEntry,
		backingReader: backingReader,
	}
}

func (sf *ShareFile) SetInode(ino *nodefs.Inode) {
	sf.ino = ino
}

func (sf *ShareFile) String() string {
	//TODO
	return "ShareFile"
}

func (sf *ShareFile) InnerFile() nodefs.File {
	return nil
}

func (sf *ShareFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	n, err := sf.backingReader.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, fuse.EIO
	}

	return fuse.ReadResultData(dest[:n]), fuse.OK
}

func (sf *ShareFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	return 0, fuse.EBADF
}

func (sf *ShareFile) Flock(flags int) fuse.Status {
	return fuse.OK
}

func (sf *ShareFile) Flush() fuse.Status {
	return fuse.OK
}

func (sf *ShareFile) Release() {
	err := sf.backingReader.Close()
	if err != nil {
		panic(err)
	}
}

func (sf *ShareFile) Fsync(flags int) (code fuse.Status) {
	return fuse.OK
}

func (sf *ShareFile) Truncate(size uint64) fuse.Status {
	return fuse.EBADF
}

func (sf *ShareFile) GetAttr(out *fuse.Attr) fuse.Status {
	out.Size = sf.fileEntry.Size
	out.Ctime = sf.fileEntry.Ctime
	out.Atime = sf.fileEntry.Etime
	out.Mtime = sf.fileEntry.Mtime
	out.Mode = FILE_MODE

	return fuse.OK
}

func (sf *ShareFile) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.EPERM
}

func (sf *ShareFile) Chmod(perms uint32) fuse.Status {
	return fuse.EPERM
}

func (sf *ShareFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.EPERM
}

func (sf *ShareFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return fuse.EBADF
}

type ShareFs struct {
	pathfs.FileSystem
	share       SpiderOakShare
	dirCacheMtx sync.Mutex
	dirCache    map[string]*DirInfo
	fileCache   *FileCache
}

func New(shareID string, roomKey string, cacheDir string, cacheSize uint64) pathfs.FileSystem {
	fc, err := NewFileCache(cacheDir, cacheSize)
	if err != nil {
		panic(err)
	}
	return &ShareFs{
		FileSystem: pathfs.NewDefaultFileSystem(),
		share:      NewSpiderOakShare(shareID, roomKey),
		dirCache:   make(map[string]*DirInfo),
		fileCache:  fc,
	}
}

func (fs *ShareFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	log.Print("GetAttr(\"/", name, "\")")
	if name == "" {
		return &fuse.Attr{
			Mode: DIR_MODE,
		}, fuse.OK
	}

	dir_path := path.Clean(path.Dir("/" + name))
	base_name := path.Base(name)
	di, _, err := fs.lookupDir(dir_path)
	if err != fuse.OK {
	}
	for _, dir := range di.Dirs {
		if dir.Name == base_name {
			return &fuse.Attr{
				Mode: DIR_MODE,
			}, fuse.OK
		}
	}

	for _, file := range di.Files {
		if file.Name == base_name {
			return &fuse.Attr{
				Mode:  FILE_MODE,
				Size:  file.Size,
				Ctime: file.Ctime,
				//@todo: what is etime?
				Atime: file.Etime,
				Mtime: file.Mtime,
			}, fuse.OK
		}
	}

	return nil, fuse.ENOENT
}

func (fs *ShareFs) lookupDir(name string) (*DirInfo, string, fuse.Status) {
	//@fixme: return ENOTDIR if a file is being opened
	parts := strings.Split(name, "/")
	url := "/"
	di, err := fs.getDirInfo(url)
	if err != fuse.OK {
		return nil, "", err
	}

	dir_found := false
	for _, part := range parts {
		if part == "" {
			continue
		}

		for _, de := range di.Dirs {
			if de.Name != part {
				continue
			}

			url += de.Url
			di, err = fs.getDirInfo(url)
			if err != fuse.OK {
				return nil, "", err
			}
			dir_found = true
			break
		}

		if !dir_found {
			return nil, "", fuse.ENOENT
		}
	}

	return di, url, fuse.OK
}

func (fs *ShareFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	log.Print("OpenDir(\"/", name, "\")")
	di, _, err := fs.lookupDir(name)
	if err != fuse.OK {
		return nil, err
	}

	entries := make([]fuse.DirEntry, 0, len(di.Dirs)+len(di.Files))
	for _, dir := range di.Dirs {
		entries = append(entries, fuse.DirEntry{
			Name: dir.Name,
			Mode: DIR_MODE,
			Ino:  0,
		})
	}
	for _, file := range di.Files {
		entries = append(entries, fuse.DirEntry{
			Name: file.Name,
			Mode: FILE_MODE,
			Ino:  0,
		})
	}

	return entries, fuse.OK
}

func (fs *ShareFs) getDirInfo(name string) (*DirInfo, fuse.Status) {
	name = path.Clean(name)
	fs.dirCacheMtx.Lock()

	if res, ok := fs.dirCache[name]; ok {
		fs.dirCacheMtx.Unlock()
		return res, fuse.OK
	}

	fs.dirCacheMtx.Unlock()
	di, err := fs.share.GetDirInfo(name)
	if err != nil {
		log.Printf("getDirInfo(\"%s\") failed: %s", name, err)
		return nil, fuse.EIO
	}

	fs.dirCacheMtx.Lock()
	fs.dirCache[name] = di
	fs.dirCacheMtx.Unlock()

	return di, fuse.OK

}

func (fs *ShareFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	log.Print("Open(\"/", name, "\")")
	if name == "" {
		return nil, fuse.EINVAL
	}

	dir_path := path.Clean(path.Dir("/" + name))
	baseName := path.Base(name)
	di, url, err := fs.lookupDir(dir_path)
	if err != fuse.OK {
		return nil, err
	}

	var entry *FileEntry = nil
	for _, file := range di.Files {
		if file.Name == baseName {
			entry = &file
			break
		}
	}

	if entry == nil {
		return nil, fuse.ENOENT
	}

	url = fs.share.GetUrlForPath(url + entry.Url)

	return NewShareFile(*entry, fs.fileCache.GetFile(*entry, url)), fuse.OK
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s: -share-id <SHAREID> -room-key <ROOMKEY> [option]... <MOUNTPOINT>\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Parse()
	if *shareID == "" || *roomKey == "" || flag.NArg() < 1 || help {
		usage()
		os.Exit(-1)
	}

	if _, err := os.Stat(*cacheDir); err != nil {
		log.Print("Can't stat cache dir: ", *cacheDir)
		os.Exit(-1)
	}

	if cacheSize == 0 {
		cacheSize = 1 << 30
		os.Exit(-1)
	}

	log.Printf("API root: %s", API_ROOT+calculateShareIDHash(*shareID)+"/"+*roomKey)
	nfs := pathfs.NewPathNodeFs(
		New(*shareID, *roomKey, *cacheDir, cacheSize), nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt)
		for {
			<-sigChan
			log.Print("Cought interrupt signal, unmounting")
			err := server.Unmount()
			if err != nil {
				log.Print("Can't unmount, staying alive: ", err)
			} else {
				log.Print("Quitting")
				os.Exit(0)
			}
		}
	}()

	defer func() {
		if err := server.Unmount(); err != nil {
			log.Fatal(err)
		}
	}()

	server.Serve()
}
