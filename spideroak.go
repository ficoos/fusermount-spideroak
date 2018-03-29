package main

import (
	"encoding/base32"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
)

const API_ROOT = "https://spideroak.com/share/"

type FileEntry struct {
	Name  string `json:"name"`
	Url   string `json:"url"`
	Size  uint64 `json:"size"`
	Ctime uint64 `json:"ctime"`
	Etime uint64 `json:"etime"`
	Mtime uint64 `json:"mtime"`
}

type DirInfo struct {
	Dirs  []DirEntry  `json:"dirs"`
	Files []FileEntry `json:"files"`
}

type DirEntry struct {
	Name string
	Url  string
}

func (de *DirEntry) UnmarshalJSON(raw []byte) error {
	var results [2]string
	err := json.Unmarshal(raw, &results)
	if err != nil {
		return err
	}

	de.Name = results[0]
	if de.Name[len(de.Name)-1] == '/' {
		de.Name = de.Name[:len(de.Name)-1]
	}
	de.Url = results[1]

	return nil
}

type SpiderOakShare struct {
	urlPrefix string
}

func NewSpiderOakShare(shareID string, roomKey string) SpiderOakShare {
	return SpiderOakShare{
		urlPrefix: API_ROOT + calculateShareIDHash(shareID) + "/" + roomKey,
	}
}

func calculateShareIDHash(shareName string) string {
	return strings.TrimRight(base32.StdEncoding.EncodeToString([]byte(shareName)), "=")
}

func (share *SpiderOakShare) GetUrlForPath(fpath string) string {
	return share.urlPrefix + path.Clean(fpath)
}

func (share *SpiderOakShare) GetDirInfo(dirPath string) (*DirInfo, error) {
	url := share.GetUrlForPath(dirPath)

	if url[len(url)-1] != '/' {
		url += "/"
	}

	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return nil, NewHttpError(res)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var data DirInfo
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	return &data, nil
}
