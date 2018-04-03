package main

import (
	"bytes"
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

const STREAM_CHUNK_SIZE = 4096 * 4

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

type BitMap []uint64

const BM_ITEM_SIZE = 64

func NewBitMap(size uint) BitMap {
	arraySize := (size + BM_ITEM_SIZE - 1) / BM_ITEM_SIZE
	bm := make([]uint64, arraySize)
	// mark the excess bits as set
	var mask uint64 = ^((1 << (size % BM_ITEM_SIZE)) - 1)
	bm[arraySize-1] = mask

	return bm
}

func (bm BitMap) Set(offset uint) {
	[]uint64(bm)[offset/BM_ITEM_SIZE] |= 1 << (offset % BM_ITEM_SIZE)
}

func (bm BitMap) Unset(offset uint) {
	[]uint64(bm)[offset/BM_ITEM_SIZE] &= ^(1 << (offset % BM_ITEM_SIZE))
}

func (bm BitMap) Get(offset uint) bool {
	return []uint64(bm)[offset/BM_ITEM_SIZE]&(1<<(offset%BM_ITEM_SIZE)) != 0
}

func (bm BitMap) Len() int {
	return len(bm) * BM_ITEM_SIZE
}

func (bn BitMap) FirstUnsetFrom(offset uint) int {
	for i := int(offset); i < len(bn); i++ {
		if bn[i] == 0xffffffff {
			continue
		}

		cell := bn[i]
		for j := 0; j < BM_ITEM_SIZE; j++ {
			if cell&1 != 1 {
				return i*BM_ITEM_SIZE + j
			} else {
				cell >>= 1
			}
		}
	}

	return -1
}

func (bn BitMap) Debug() string {
	var res bytes.Buffer
	for i := 0; i < len(bn); i++ {
		for j := uint(0); j < BM_ITEM_SIZE/4; j++ {
			b := ([]uint64(bn)[i] >> (j * 4)) & 0x0f
			switch b {
			case 0:
				res.WriteString(" ")
			case 1:
				res.WriteString("\u2596")
			case 2:
				res.WriteString("\u2597")
			case 3:
				res.WriteString("\u2584")
			case 4:
				res.WriteString("\u2598")
			case 5:
				res.WriteString("\u258C")
			case 6:
				res.WriteString("\u259A")
			case 7:
				res.WriteString("\u2599")
			case 8:
				res.WriteString("\u259D")
			case 9:
				res.WriteString("\u259E")
			case 10:
				res.WriteString("\u2590")
			case 11:
				res.WriteString("\u259F")
			case 12:
				res.WriteString("\u2580")
			case 13:
				res.WriteString("\u259B")
			case 14:
				res.WriteString("\u259C")
			case 15:
				res.WriteString("\u2588")
			}
		}
	}

	return res.String()
}

func (bn BitMap) FirstUnset() int {
	return bn.FirstUnsetFrom(0)
}

type StreamingFile struct {
	url            string
	localPath      string
	backingFile    *os.File
	bitmap         BitMap
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
		backingFile:    backingFile,
		fileSize:       fileSize,
		bitmap:         NewBitMap(uint((fileSize + STREAM_CHUNK_SIZE - 1) / STREAM_CHUNK_SIZE)),
		refCount:       2, // the first is the returned file the seconds is the completion waiting goroutine
		bytesWriteCond: sync.NewCond(&sync.Mutex{}),
	}

	if fileSize < STREAM_CHUNK_SIZE {
		go f.download(0)
	} else {
		tailPart := STREAM_CHUNK_SIZE + (fileSize % STREAM_CHUNK_SIZE)
		go f.download(0)
		go f.download(uint(fileSize - tailPart))
	}
	go func() {
		f.bytesWriteCond.L.Lock()
		defer f.bytesWriteCond.L.Unlock()
		for f.bitmap.FirstUnset() != -1 {
			f.bytesWriteCond.Wait()
		}

		if f.bitmap.FirstUnset() == -1 {
			if err := os.Rename(f.localPath+".part", f.localPath); err != nil {
				panic(err)
			}
			if err := f.Close(); err != nil {
				panic(err)
			}

			log.Printf("Finished downloading '%s'", f.url)
		}
	}()

	return f
}

func (sf *StreamingFile) markChunkComplete(chunk uint) {
	sf.bytesWriteCond.L.Lock()
	sf.bitmap.Set(chunk)
	sf.bytesWriteCond.Broadcast()
	sf.bytesWriteCond.L.Unlock()
}

func (sf *StreamingFile) download(fromOffset uint) {
	if !sf.IncRef() {
		panic("Download requested after file finished downloading")
	}
	defer func() {
		if err := sf.Close(); err != nil {
			panic(err)
		}
	}()

	buff := make([]byte, STREAM_CHUNK_SIZE)
	currentChunk := fromOffset / STREAM_CHUNK_SIZE
	client := &http.Client{}
	bytesWritten := uint(0)
	for {
		if currentChunk == uint(sf.bitmap.Len()) || sf.bitmap.Get(currentChunk) {
			return
		}
		log.Printf("Downloading '%s+%d'", sf.url, fromOffset)
		request, err := http.NewRequest(http.MethodGet, sf.url, nil)
		if err != nil {
			panic(err)
		}

		request.Header.Add("Range", fmt.Sprintf("bytes=%d-", currentChunk*STREAM_CHUNK_SIZE+bytesWritten))
		resp, err := client.Do(request)
		if err != nil {
			log.Printf("Failed to get '%s': %s (%d)", sf.url, resp.Status, resp.StatusCode)
			continue
		}

		switch resp.StatusCode {
		case http.StatusOK, http.StatusPartialContent:
			break
		case http.StatusRequestedRangeNotSatisfiable:
			panic(fmt.Sprintf("Got requested range not satisfiable for range %d", currentChunk*STREAM_CHUNK_SIZE))
		default:
			log.Printf("Failed to get '%s': %s (%d)", sf.url, resp.Status, resp.StatusCode)
		}

		for {
			nread, err := resp.Body.Read(buff)
			if err != nil && err != io.EOF {
				log.Printf("Error while downloading '%s', retrying", err)
				break
			}

			if _, err := sf.backingFile.WriteAt(buff[:nread], int64(currentChunk*STREAM_CHUNK_SIZE+uint(bytesWritten))); err != nil {
				panic(err)
			}

			bytesWritten += uint(nread)
			for bytesWritten >= STREAM_CHUNK_SIZE {
				bytesWritten -= STREAM_CHUNK_SIZE
				sf.markChunkComplete(currentChunk)
				currentChunk++
				if currentChunk == uint(sf.bitmap.Len()) || sf.bitmap.Get(currentChunk) {
					return
				}
			}

			if uint64(currentChunk*STREAM_CHUNK_SIZE+bytesWritten) >= sf.fileSize {
				sf.markChunkComplete(currentChunk)
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
	for i := 0; i < (len(p)+STREAM_CHUNK_SIZE-1)/STREAM_CHUNK_SIZE; i++ {
		requiredChunk := uint(off/STREAM_CHUNK_SIZE + int64(i))
		for !sf.bitmap.Get(requiredChunk) {
			sf.bytesWriteCond.Wait()
		}
	}
	sf.bytesWriteCond.L.Unlock()

	return sf.backingFile.ReadAt(p, off)
}

func (sf *StreamingFile) Close() error {
	if atomic.AddInt64(&sf.refCount, -1) == 0 {
		if err := sf.backingFile.Close(); err != nil {
			panic(err)
		}
	}

	return nil
}
