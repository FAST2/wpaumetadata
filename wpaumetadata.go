package wpaumetadata

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/ncw/swift/v2"

	"crypto/md5"
	"encoding/hex"
)

type Jobinfo struct {
	JobId  string `json:"jobId"`
	Status string `json:"status"`
}

type Jobinfos struct {
	Infos []Jobinfo `json:"jobs"`
}

const FILENAME = "jobs.json"

func GetMetadata(ctx context.Context, c swift.Connection, container string) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	_, err := c.ObjectGet(ctx, container, FILENAME, buf, true, nil)
	if err != nil {
		return buf, err
	}
	return buf, nil
}

func ParseMetadata(buf *bytes.Buffer) (Jobinfos, error) {
	infos := Jobinfos{}
	err := json.Unmarshal(buf.Bytes(), &infos)
	if err != nil {
		return infos, err
	}
	return infos, nil
}

func Upload(ctx context.Context, c swift.Connection, container string, content []byte) {
	hasher := md5.New()
	hasher.Write(content)
	md5hash := hex.EncodeToString(hasher.Sum(nil))

	file, err := c.ObjectCreate(ctx, container, FILENAME, false, md5hash, "json", nil)
	if err != nil {
		panic(err)
	} else {
		file.Write(content)
	}
	file.Close()
}

func Add(ctx context.Context, c swift.Connection, container string, jobId string, status string) {
	buf, err := GetMetadata(ctx, c, container)

	info := Jobinfo{jobId, status}
	var content []byte = nil

	if err != nil {
		println("jobs.json doesn't exists, will create new file")
		// Does not exists, corruped or otherwise, recreate it
		infos := Jobinfos{}
		infos.Infos = append(infos.Infos, info)
		b, err := json.Marshal(infos)
		if err != nil {
			println("Couldn't marshal json data, won't create jobs.json")
			return
		} else {
			content = b
		}
	} else {
		println("File exists, will append to it")
		infos := Jobinfos{}
		err := json.Unmarshal(buf.Bytes(), &infos)
		if err != nil {
			println("Couldn't unmarshal current json data, won't update jobs.json")
			return
		}
		infos.Infos = append(infos.Infos, info)
		b, err := json.Marshal(infos)
		if err != nil {
			println("Couldn't marshal json data, won't create jobs.json")
			return
		} else {
			content = b
		}
	}

	Upload(ctx, c, container, content)
}
