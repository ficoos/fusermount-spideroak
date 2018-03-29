package main

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

const STREAM_CHUNK_SIZE = 4096

type HttpError struct {
	Status     string
	StatusCode int
}

func hashPath(path string) string {
	hash := sha512.Sum512([]byte(path))
	return hex.EncodeToString(hash[:])
}

func NewHttpError(response *http.Response) *HttpError {
	return &HttpError{
		Status:     response.Status,
		StatusCode: response.StatusCode,
	}
}

func (e *HttpError) String() string {
	return fmt.Sprintf("Bad status code: %s (%d)", e.Status, e.StatusCode)
}

func (e *HttpError) Error() string {
	return e.String()
}

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type FileCache struct {
	share              *SpiderOakShare
	idCounter          uint64
	baseDir            string
	cacheSize          uint64
	activeDownloadsMtx sync.Mutex
	activeDownloads    map[string]*StreamingFile
	compactChan        chan int
}

func NewFileCache(baseDir string, cacheSize uint64) (*FileCache, error) {
	if _, err := os.Stat(baseDir); err != nil {
		return nil, err
	}

	fc := &FileCache{
		idCounter:       0,
		baseDir:         baseDir,
		cacheSize:       cacheSize,
		activeDownloads: make(map[string]*StreamingFile),
		compactChan:     make(chan int, 1),
	}

	fc.compactChan <- 1
	go fc.compactCache()
	return fc, nil
}

type _FileSorter struct {
	l []os.FileInfo
}

func (s *_FileSorter) Len() int {
	return len(s.l)
}

// Swap is part of sort.Interface.
func (s *_FileSorter) Swap(i, j int) {
	s.l[i], s.l[j] = s.l[j], s.l[i]
}

func (s *_FileSorter) Less(i, j int) bool {
	//TODO: sort by atime instead of mtime
	return s.l[i].ModTime().After(s.l[j].ModTime())
}

func (fc *FileCache) compactCache() {
	for {
		v := <-fc.compactChan
		if v == 0 {
			break
		}
		files, err := ioutil.ReadDir(fc.baseDir)
		if err != nil {
			panic(err)
		}

		var totalSize uint64 = 0

		sort.Sort(&_FileSorter{files})

		for _, f := range files {
			if strings.Contains(f.Name(), ".part") {
				continue
			}

			if totalSize+uint64(f.Size()) > fc.cacheSize {
				log.Print("Compacting cache")
				if err := os.Remove(fc.baseDir + "/" + f.Name()); err != nil {
					panic(err)
				}
			} else {
				totalSize += uint64(f.Size())
			}
		}
	}
}

func (fc *FileCache) GetFile(entry FileEntry, url string) ReadAtCloser {
	select {
	case fc.compactChan <- 1:
	default:
	}
	localPath := fc.baseDir + "/" + hashPath(url)
	fc.activeDownloadsMtx.Lock()
	defer fc.activeDownloadsMtx.Unlock()

	for {
		// Check if we already have the file cached
		if f, err := os.Open(localPath); err == nil {
			stat, err := f.Stat()
			if err != nil || uint64(stat.Size()) == entry.Size {
				return f
			} else {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}
		}

		if sf, ok := fc.activeDownloads[url]; ok {
			if sf.IncRef() {
				return sf
			} else {
				delete(fc.activeDownloads, url)
				//retry
				continue
			}
		} else {
			fe := NewStreamingFile(localPath, url, entry.Size)
			fc.activeDownloads[url] = fe

			return fe
		}
	}
}

type StreamingFile struct {
	url            string
	localPath      string
	response       *http.Response
	backingFile    *os.File
	bytesWritten   uint64
	fileSize       uint64
	refCount       int64
	bytesWriteCond *sync.Cond
}

func NewStreamingFile(localPath string, url string, fileSize uint64) *StreamingFile {
	tmpFile := localPath + ".part"

	backingFile, err := os.Create(tmpFile)
	if err != nil {
		panic(err)
	}

	f := &StreamingFile{
		url:            url,
		localPath:      localPath,
		response:       nil,
		backingFile:    backingFile,
		fileSize:       fileSize,
		bytesWritten:   0,
		refCount:       2, // 1 for the download thread and one for the return value
		bytesWriteCond: sync.NewCond(&sync.Mutex{}),
	}

	go f.stream()

	return f
}

func (sf *StreamingFile) stream() {
	log.Printf("Downloading '%s'", sf.url)
	buff := make([]byte, STREAM_CHUNK_SIZE)
	for {
		resp, err := http.Get(sf.url)
		sf.bytesWritten = 0
		if err != nil || resp.StatusCode != http.StatusOK {
			log.Printf("Failed to get '%s'", sf.url)
			continue
		}

		for {
			nread, err := resp.Body.Read(buff)
			if err != nil && err != io.EOF {
				log.Printf("Error while downloading '%s', retrying", err)
				break
			}

			if _, err := sf.backingFile.Write(buff[:nread]); err != nil {
				panic(err)
			}

			sf.bytesWriteCond.L.Lock()
			sf.bytesWritten += uint64(nread)
			sf.bytesWriteCond.Broadcast()
			sf.bytesWriteCond.L.Unlock()
			if err == io.EOF {
				if err := os.Rename(sf.localPath+".part", sf.localPath); err != nil {
					panic(err)
				}

				if err := sf.Close(); err != nil {
					panic(err)
				}

				log.Printf("Finished downloading '%s'", sf.url)
				return
			}
		}
	}
}

func (sf *StreamingFile) IncRef() bool {
	for {
		v := atomic.LoadInt64(&sf.refCount)
		if v > 0 {
			if atomic.CompareAndSwapInt64(&sf.refCount, v, v+1) {
				return true
			} else {
				continue
			}
		} else {
			return false
		}
	}
}

func (sf *StreamingFile) ReadAt(p []byte, off int64) (int, error) {
	sf.bytesWriteCond.L.Lock()
	for sf.bytesWritten < uint64(off+int64(len(p))) && sf.bytesWritten < sf.fileSize {
		sf.bytesWriteCond.Wait()
	}
	sf.bytesWriteCond.L.Unlock()

	return sf.backingFile.ReadAt(p, off)
}

func (sf *StreamingFile) Close() error {
	v := atomic.AddInt64(&sf.refCount, -1)
	if v == 0 {
		if err := sf.backingFile.Close(); err != nil {
			panic(err)
		} else {
			return nil
		}
	}

	return nil
}
